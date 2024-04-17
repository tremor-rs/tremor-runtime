use std::{io, ops::RangeInclusive};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::Mutex,
};

/// Find free TCP port for use in test server endpoints
struct FreePort {
    port: u16,
}

impl FreePort {
    const RANGE: RangeInclusive<u16> = 10000..=65535;

    /// Create a new free port finder
    #[must_use]
    pub const fn new() -> Self {
        Self {
            port: *Self::RANGE.start(),
        }
    }

    /// Find the next free port
    ///
    /// # Errors
    /// If no free port could be found
    async fn next(&mut self) -> io::Result<u16> {
        let mut candidate = self.port;
        let inc: u16 = rand::random();
        self.port = self.port.wrapping_add(inc % 420);
        loop {
            if let Ok(listener) = TcpListener::bind(("127.0.0.1", candidate)).await {
                let port = listener.local_addr()?.port();
                drop(listener);
                return Ok(port);
            }
            candidate = self.port;
            self.port = self.port.wrapping_add(1).min(*Self::RANGE.end());
        }
    }
}

lazy_static::lazy_static! {
    static ref FREE_PORT: Mutex<FreePort> = Mutex::new(FreePort::new());
}
/// Find free TCP port for use in test server endpoints
/// # Errors
/// If no free port could be found
pub async fn find_free_tcp_port() -> io::Result<u16> {
    FREE_PORT.lock().await.next().await
}
/// Find free UDP port for use in test server endpoints
/// # Errors
/// If no free port could be found
pub async fn find_free_udp_port() -> io::Result<u16> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let port = socket.local_addr()?.port();
    drop(socket);
    Ok(port)
}
