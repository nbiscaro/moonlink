pub fn init_logging() {
    use std::io;
    use tracing_subscriber::{
        fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
    };

    // base registry
    let registry = Registry::default()
        .with(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")), // fallback
        )
        .with(fmt::layer().with_writer(io::stderr).with_ansi(false));

    #[cfg(feature = "profiling")]
    let registry = registry.with(console_subscriber::spawn());

    let _ = registry.try_init();
}
