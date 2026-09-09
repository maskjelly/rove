use axum::{Router, response::Sse, response::sse::Event, routing::get};
use chrono::Utc;
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::IntervalStream;


fn get_data() -> String {
    format!(
        "New data from the server at: {}",
        Utc::now().format("%d/%m/%Y %H:%M:%S")
    )
}


async fn sse_event_handler() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let interval = tokio::time::interval(Duration::from_secs(5));
    let stream = IntervalStream::new(interval).map(|_| Ok(Event::default().data(get_data())));

    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(5)))
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/events", get(sse_event_handler));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    println!("Listening...");

    axum::serve(listener, app).await.unwrap();
}
