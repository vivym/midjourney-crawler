use std::env;
use std::sync::Arc;

use anyhow::Result;
use futures::{future, stream, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use reqwest_tracing::TracingMiddleware;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
};
use tokio_stream::wrappers::LinesStream;
use tracing::{debug, warn, instrument};
use serde::de::{self, Deserialize, Deserializer, Visitor};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    HTTPError(#[from] reqwest::Error),
}

/// Define the options for a progress bar.
#[derive(Debug, Clone)]
pub struct ProgressBarOpts {
    /// Progress bar template string.
    template: Option<String>,
    /// Progression characters set.
    ///
    /// There must be at least 3 characters for the following states:
    /// "filled", "current", and "to do".
    progress_chars: Option<String>,
    /// Enable or disable the progress bar.
    enabled: bool,
    /// Clear the progress bar once completed.
    clear: bool,
}

impl Default for ProgressBarOpts {
    fn default() -> Self {
        Self {
            template: None,
            progress_chars: None,
            enabled: true,
            clear: true,
        }
    }
}

impl ProgressBarOpts {
    /// Template representing the bar and its position.
    ///
    ///`███████████████████████████████████████ 11/12 (99%) eta 00:00:02`
    pub const TEMPLATE_BAR_WITH_POSITION: &'static str =
        "{bar:40.blue} {pos:>}/{len} ({percent}%) eta {eta_precise:.blue}";
    /// Template which looks like the Python package installer pip.
    ///
    /// `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 211.23 KiB/211.23 KiB 1008.31 KiB/s eta 0s`
    pub const TEMPLATE_PIP: &'static str =
        "{bar:40.green/black} {bytes:>11.green}/{total_bytes:<11.green} {bytes_per_sec:>13.red} eta {eta:.blue}";
    /// Use increasing quarter blocks as progress characters: `"█▛▌▖  "`.
    pub const CHARS_BLOCKY: &'static str = "█▛▌▖  ";
    /// Use fade-in blocks as progress characters: `"█▓▒░  "`.
    pub const CHARS_FADE_IN: &'static str = "█▓▒░  ";
    /// Use fine blocks as progress characters: `"█▉▊▋▌▍▎▏  "`.
    pub const CHARS_FINE: &'static str = "█▉▊▋▌▍▎▏  ";
    /// Use a line as progress characters: `"━╾─"`.
    pub const CHARS_LINE: &'static str = "━╾╴─";
    /// Use rough blocks as progress characters: `"█  "`.
    pub const CHARS_ROUGH: &'static str = "█  ";
    /// Use increasing height blocks as progress characters: `"█▇▆▅▄▃▂▁  "`.
    pub const CHARS_VERTICAL: &'static str = "█▇▆▅▄▃▂▁  ";

    /// Create a new [`ProgressBarOpts`].
    pub fn new(
        template: Option<String>,
        progress_chars: Option<String>,
        enabled: bool,
        clear: bool,
    ) -> Self {
        Self {
            template,
            progress_chars,
            enabled,
            clear,
        }
    }

    /// Create a [`ProgressStyle`] based on the provided options.
    pub fn to_progress_style(self) -> ProgressStyle {
        let mut style = ProgressStyle::default_bar();
        if let Some(template) = self.template {
            style = style.template(&template).unwrap();
        }
        if let Some(progress_chars) = self.progress_chars {
            style = style.progress_chars(&progress_chars);
        }
        style
    }

    /// Create a [`ProgressBar`] based on the provided options.
    pub fn to_progress_bar(self, len: u64) -> ProgressBar {
        // Return a hidden Progress bar if we disabled it.
        if !self.enabled {
            return ProgressBar::hidden();
        }

        // Otherwise returns a ProgressBar with the style.
        let style = self.to_progress_style();
        ProgressBar::new(len).with_style(style)
    }

    /// Create a new [`ProgressBarOpts`] which looks like Python pip.
    pub fn with_pip_style() -> Self {
        Self {
            template: Some(ProgressBarOpts::TEMPLATE_PIP.into()),
            progress_chars: Some(ProgressBarOpts::CHARS_LINE.into()),
            enabled: true,
            clear: true,
        }
    }

    /// Set to `true` to clear the progress bar upon completion.
    pub fn set_clear(&mut self, clear: bool) {
        self.clear = clear;
    }

    /// Create a new [`ProgressBarOpts`] which hides the progress bars.
    pub fn hidden() -> Self {
        Self {
            enabled: false,
            ..ProgressBarOpts::default()
        }
    }
}

/// Define the ProgressBar options.
///
/// By default, the main progress bar will stay on the screen upon completion,
/// but the child ones will be cleared once complete.
#[derive(Debug, Clone)]
pub struct StyleOptions {
    /// Style options for the main progress bar.
    main: ProgressBarOpts,
    /// Style options for the child progress bar(s).
    child: ProgressBarOpts,
}

impl Default for StyleOptions {
    fn default() -> Self {
        Self {
            main: ProgressBarOpts {
                template: Some(ProgressBarOpts::TEMPLATE_BAR_WITH_POSITION.into()),
                progress_chars: Some(ProgressBarOpts::CHARS_FINE.into()),
                enabled: true,
                clear: false,
            },
            child: ProgressBarOpts::with_pip_style(),
        }
    }
}

impl StyleOptions {
    /// Create new [`Downloader`] [`StyleOptions`].
    pub fn new(main: ProgressBarOpts, child: ProgressBarOpts) -> Self {
        Self { main, child }
    }

    /// Set the options for the main progress bar.
    pub fn set_main(&mut self, main: ProgressBarOpts) {
        self.main = main;
    }

    /// Set the options for the child progress bar.
    pub fn set_child(&mut self, child: ProgressBarOpts) {
        self.child = child;
    }

    /// Return `false` if neither the main nor the child bar is enabled.
    pub fn is_enabled(self) -> bool {
        self.main.enabled || self.child.enabled
    }
}

#[derive(Debug)]
pub struct Snowflake(u64);

impl<'de> Deserialize<'de> for Snowflake {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct IdVisitor;

        impl<'de> Visitor<'de> for IdVisitor {
            type Value = Snowflake;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("user ID as a number or string")
            }

            fn visit_u64<E>(self, id: u64) -> Result<Self::Value, E>
                where E: de::Error
            {
                Ok(Snowflake(id))
            }

            fn visit_str<E>(self, id: &str) -> Result<Self::Value, E>
                where E: de::Error
            {
                id.parse().map(Snowflake).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(IdVisitor)
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct Attachment {
    pub id: Snowflake,
    pub filename: String,
    pub content_type: Option<String>,
    pub size: Option<u64>,
    pub url: String,
    pub proxy_url: Option<String>,
    pub height: Option<u64>,
    pub width: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Author {
    pub id: Snowflake,
    pub username: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct Message {
    pub id: Snowflake,
    pub channel_id: Snowflake,
    pub author: Author,
    pub content: String,
    pub attachments: Vec<Attachment>,
    #[serde(rename = "type")]
    pub type_: u8,
}

#[instrument]
async fn fetch_attachment(
    http_client: &ClientWithMiddleware,
    attachment: Attachment,
    multi_progress: Arc<MultiProgress>,
    main_progress: Arc<ProgressBar>,
    child_style: ProgressBarOpts,
) -> Result<usize> {
    debug!("Fetching attachment {}", attachment.filename);

    let mut response = http_client.get(&attachment.url).send().await?;
    response.error_for_status_ref()?;

    let total_size = response.content_length().unwrap_or(0);
    let pb = multi_progress.add(
        child_style.to_progress_bar(total_size)
    );

    let ext = attachment
        .filename
        .split('.')
        .last()
        .ok_or(Error::InvalidUrl(attachment.url.clone()))?;
    let file_name = format!("images/{}.{}", attachment.id.0, ext);
    let mut file = File::create(file_name).await?;
    let mut size: usize = 0;

    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk).await?;
        size += chunk.len();
        pb.inc(chunk.len() as u64);
    }

    pb.finish_and_clear();

    main_progress.inc(1);

    Ok(size)
}

#[tokio::main]
async fn main() {
    let max_concurrency = 128;
    let token = env::var("DISCORD_TOKEN").unwrap();

    let file_appender = tracing_appender::rolling::daily("logs", "mj-crawler.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .init();

    let retry_policy = ExponentialBackoff::builder()
        .build_with_max_retries(5);

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&token).unwrap(),
    );

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();

    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .with(TracingMiddleware::default())
        .build();

    let multi_progress = Arc::new(MultiProgress::new());
    let style_options = StyleOptions::default();
    let main_progress = Arc::new(
        multi_progress.add(style_options.main.clone().to_progress_bar(1000000))
    );
    main_progress.tick();

    let file = File::open("messages.jsonl").await.unwrap();
    let reader = BufReader::new(file);
    let download_stream = LinesStream::new(reader.lines())
        .map(|line| {
            let line = line.unwrap();
            if let Ok(message) = serde_json::from_str::<Message>(&line) {
                return Some(message);
            } else {
                return None;
            }
        })
        .filter(|message| {
            let predicate = if let Some(message) = message {
                message.author.id.0 == 936929561302675456 && message.attachments.len() > 0
            } else {
                false
            };
            return future::ready(predicate);
        })
        .map(|message| message.unwrap())
        .map(|message| stream::iter(message.attachments))
        .flatten_unordered(None)
        .map(|attachment| {
            fetch_attachment(
                &client,
                attachment,
                multi_progress.clone(),
                main_progress.clone(),
                style_options.child.clone()
            )
        })
        .buffer_unordered(max_concurrency);

    // consume the stream
    let mut download_stream = download_stream.fuse();
    while let Some(result) = download_stream.next().await {
        // do nothing
        match result {
            Ok(size) => {
                debug!("Downloaded {} bytes", size);
            }
            Err(e) => {
                warn!("Error: {}", e);
            }
        }
    }
}
