#![deny(unsafe_code)]
#![deny(clippy::pedantic)]

use std::{
    fs::File,
    io::{BufWriter, Write},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use glommio::{net::UdpSocket, prelude::*};
use tracing::instrument;

use crate::args::{setup_logger, Args};

pub mod args;

fn main() {
    let args = Arc::new(Args::parse());

    setup_logger(&args.logging_format);

    tracing::info!(?args);

    let placement = PoolPlacement::MaxSpread(1, None);

    let results = LocalExecutorPoolBuilder::new(placement)
        .io_memory(2 << 32)
        .ring_depth(4096)
        .spin_before_park(Duration::from_secs(1))
        .on_all_shards(|| async move {
            let args = args.clone();
            return server_main(args).await;
        })
        .expect("failed to spawn executor")
        .join_all();

    for result in results {
        result.unwrap().unwrap();
    }
}

type MessageInfoBuf = [u8; 16];

#[instrument(skip(args))]
async fn server_main(args: Arc<Args>) -> Result<(), GlommioError<()>> {
    tracing::info!("Server started");

    let socket_addr = SocketAddr::new(args.address, args.port);
    let socket = UdpSocket::bind(socket_addr).unwrap();

    let mut results: Vec<MessageInfoBuf> =
        Vec::with_capacity(args.test_duration.as_millis() as usize * args.workers);

    let mut recv_buffer = vec![0u8; args.packet_size];

    socket.recv_from(&mut recv_buffer).await.unwrap();
    let mut buf = MessageInfoBuf::default();
    buf.copy_from_slice(&recv_buffer[..16]);
    results.push(buf);

    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    tracing::info!("Server ready");

    while socket.recv_from(&mut recv_buffer).await.is_ok() {
        let mut buf = MessageInfoBuf::default();
        buf.copy_from_slice(&recv_buffer[..std::mem::size_of::<u64>() * 2]);
        results.push(buf);
    }

    let results = results.iter_mut().map(|a| {
        [
            u64::from_le_bytes([a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]]),
            u64::from_le_bytes([a[8], a[9], a[10], a[11], a[12], a[13], a[14], a[15]]),
        ]
    });

    let output_file_name = &args.output_path;

    let mut f = BufWriter::new(File::create(output_file_name).unwrap());

    writeln!(f, "worker_id,sequence_id").unwrap();

    for [id, seq] in results {
        writeln!(f, "{id},{seq}").unwrap();
    }

    f.flush().unwrap();

    tracing::info!("Wrote results to {}", output_file_name.display());

    Ok(())
}
