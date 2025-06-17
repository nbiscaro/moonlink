use tracing_subscriber::Layer;

pub fn init_logging() {
    use tracing_subscriber::{
        fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
    };

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer()
        .with_test_writer()
        .with_ansi(false)
        .with_filter(env_filter);

    // Compose the subscriber.
    let subscriber = Registry::default().with(fmt_layer);

    #[cfg(feature = "profiling")]
    let subscriber = subscriber.with(console_subscriber::spawn());

    let _ = subscriber.try_init();
}
