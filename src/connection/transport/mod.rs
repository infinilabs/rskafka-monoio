mod sasl;

pub use sasl::{Credentials, SaslConfig};

#[cfg(feature = "transport-tls")]
use std::sync::Arc;
use thiserror::Error;

#[cfg(feature = "transport-tls")]
use tokio_rustls::{client::TlsStream, TlsConnector};

use monoio::io::Split;
use monoio::{
    io::{AsyncReadRent, AsyncWriteRent},
    net::tcp::TcpStream,
};

#[cfg(feature = "transport-tls")]
pub type TlsConfig = Option<Arc<rustls::ClientConfig>>;

#[cfg(not(feature = "transport-tls"))]
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone, Default)]
pub struct TlsConfig();

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Invalid host-port string: {0}")]
    InvalidHostPort(String),

    #[error("Invalid port: {0}")]
    InvalidPort(#[from] std::num::ParseIntError),

    #[cfg(feature = "transport-tls")]
    #[error("Invalid Hostname: {0}")]
    BadHostname(#[from] rustls::pki_types::InvalidDnsNameError),

    #[cfg(feature = "transport-socks5")]
    #[error("Cannot establish SOCKS5 connection: {0}")]
    Socks5(#[from] async_socks5::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "transport-tls")]
#[derive(Debug)]
pub enum Transport {
    Plain {
        inner: TcpStream,
    },

    Tls {
        inner: Pin<Box<TlsStream<TcpStream>>>,
    },
}

#[cfg(not(feature = "transport-tls"))]
#[derive(Debug)]
pub enum Transport {
    Plain { inner: TcpStream },
}

impl AsyncReadRent for Transport {
    async fn read<T: monoio::buf::IoBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        match self {
            Self::Plain { inner } => inner.read(buf).await,
            #[cfg(feature = "transport-tls")]
            Self::Tls => {
                unimplemented!()
            }
        }
    }

    async fn readv<T: monoio::buf::IoVecBufMut>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        match self {
            Self::Plain { inner } => inner.readv(buf).await,
            #[cfg(feature = "transport-tls")]
            Self::Tls => {
                unimplemented!()
            }
        }
    }
}

impl AsyncWriteRent for Transport {
    async fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
        match self {
            Self::Plain { inner } => inner.write(buf).await,
            #[cfg(feature = "transport-tls")]
            Self::Tls => {
                unimplemented!()
            }
        }
    }

    async fn writev<T: monoio::buf::IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> monoio::BufResult<usize, T> {
        match self {
            Self::Plain { inner } => inner.writev(buf_vec).await,
            #[cfg(feature = "transport-tls")]
            Self::Tls => {
                unimplemented!()
            }
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain { inner } => inner.flush().await,
            #[cfg(feature = "transport-tls")]
            Self::Tls => {
                unimplemented!()
            }
        }
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain { inner } => inner.shutdown().await,
            #[cfg(feature = "transport-tls")]
            Self::Tls => {
                unimplemented!()
            }
        }
    }
}

impl Transport {
    pub async fn connect(
        broker: &str,
        tls_config: TlsConfig,
        socks5_proxy: Option<String>,
    ) -> Result<Self> {
        let tcp_stream = Self::connect_tcp(broker, socks5_proxy).await?;
        Self::wrap_tls(tcp_stream, broker, tls_config).await
    }

    #[cfg(feature = "transport-socks5")]
    async fn connect_tcp(broker: &str, socks5_proxy: Option<String>) -> Result<TcpStream> {
        use async_socks5::connect;

        match socks5_proxy {
            Some(proxy) => {
                let mut stream = TcpStream::connect(proxy).await?;

                let mut broker_iter = broker.split(':');
                let broker_host = broker_iter
                    .next()
                    .ok_or_else(|| Error::InvalidHostPort(broker.to_owned()))?;
                let broker_port: u16 = broker_iter
                    .next()
                    .ok_or_else(|| Error::InvalidHostPort(broker.to_owned()))?
                    .parse()?;

                connect(&mut stream, (broker_host, broker_port), None).await?;

                Ok(stream)
            }
            None => Ok(TcpStream::connect(broker).await?),
        }
    }

    #[cfg(not(feature = "transport-socks5"))]
    async fn connect_tcp(broker: &str, _socks5_proxy: Option<String>) -> Result<TcpStream> {
        let connect_result = TcpStream::connect(broker).await;
        Ok(connect_result?)
    }

    #[cfg(feature = "transport-tls")]
    async fn wrap_tls(tcp_stream: TcpStream, broker: &str, tls_config: TlsConfig) -> Result<Self> {
        match tls_config {
            Some(config) => {
                // Strip port if any
                let host = broker
                    .split(':')
                    .next()
                    .ok_or_else(|| Error::InvalidHostPort(broker.to_owned()))?
                    .to_owned();
                let server_name = rustls::pki_types::ServerName::try_from(host)?;

                let connector = TlsConnector::from(config);
                let tls_stream = connector.connect(server_name, tcp_stream).await?;
                Ok(Self::Tls {
                    inner: Box::pin(tls_stream),
                })
            }
            None => Ok(Self::Plain { inner: tcp_stream }),
        }
    }

    #[cfg(not(feature = "transport-tls"))]
    async fn wrap_tls(
        tcp_stream: TcpStream,
        _broker: &str,
        _tls_config: TlsConfig,
    ) -> Result<Self> {
        Ok(Self::Plain { inner: tcp_stream })
    }
}

/// SAFETY:
///
/// `Transport` is just a wrapper of `TcpStream` when feature `transport-tls`
/// is disabled, and `TcpStream` can be safely split, so we are safe.
#[allow(unsafe_code)]
unsafe impl Split for Transport {}
