//! `DownloadMode::Streamed`: only what open streams are about to read is downloaded.

use std::{
    io::SeekFrom,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    time::timeout,
};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, DownloadMode, ManagedTorrent, Session,
    SessionOptions, SessionPersistenceConfig, create_torrent,
    listen::ListenerOptions,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
};

const PIECE: u32 = 16 * 1024;
const FILE_LEN: usize = 64 * PIECE as usize;

struct Seeded {
    _files: TempDir,
    _seeder: Arc<Session>,
    torrent: bytes::Bytes,
    peer: SocketAddr,
}

/// A session on loopback seeding a 64-piece single-file torrent.
async fn seeded() -> Seeded {
    let files = create_default_random_dir_with_torrents(1, FILE_LEN, Some("test_streamed_mode"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    let seeder = Session::new_with_opts(
        files.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    timeout(
        Duration::from_secs(10),
        seeder
            .add_torrent(
                AddTorrent::from_bytes(torrent.as_bytes().unwrap()),
                Some(AddTorrentOptions {
                    output_folder: Some(files.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap()
            .wait_until_completed(),
    )
    .await
    .unwrap()
    .unwrap();
    let peer = seeder.listen_addr().unwrap();
    Seeded {
        torrent: torrent.as_bytes().unwrap(),
        peer,
        _files: files,
        _seeder: seeder,
    }
}

async fn client(dir: &TempDir, persistence: bool) -> Arc<Session> {
    Session::new_with_opts(
        dir.path().join("out"),
        SessionOptions {
            dht: None,
            persistence: persistence.then(|| SessionPersistenceConfig::Json {
                folder: Some(dir.path().join("session")),
            }),
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn add_streamed(
    session: &Arc<Session>,
    torrent: &bytes::Bytes,
    peer: Option<SocketAddr>,
) -> Arc<ManagedTorrent> {
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent.clone()),
            Some(AddTorrentOptions {
                initial_peers: peer.map(|p| vec![p]),
                download_mode: DownloadMode::Streamed,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap()
}

fn downloaded_pieces(handle: &ManagedTorrent) -> u64 {
    handle.stats().progress_bytes.div_ceil(PIECE as u64)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_streamed_mode_downloads_only_what_streams_read() {
    setup_test_logging();
    let seeded = seeded().await;
    let dir = TempDir::with_prefix("test_streamed_mode_client").unwrap();
    let session = client(&dir, false).await;
    let handle = add_streamed(&session, &seeded.torrent, Some(seeded.peer)).await;
    handle.wait_until_initialized().await.unwrap();

    // Connected to a seeder, but no stream is open: nothing is wanted.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        downloaded_pieces(&handle),
        0,
        "nothing should download without a stream"
    );

    // A stream at the start with a two-piece lookahead.
    let mut stream = handle.clone().stream(0).await.unwrap();
    stream.set_lookahead(2 * PIECE as u64);
    let mut byte = [0u8; 1];
    timeout(Duration::from_secs(5), stream.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after_start = downloaded_pieces(&handle);
    assert!(
        (1..=3).contains(&after_start),
        "expected only the stream's window (≤ 3 pieces), got {after_start}"
    );

    // Seek far ahead. Idle peers must be woken (rather than waiting out their 5s
    // timeout), so this is served quickly.
    let started = Instant::now();
    stream
        .seek(SeekFrom::Start(48 * PIECE as u64))
        .await
        .unwrap();
    timeout(Duration::from_secs(5), stream.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    let seek_latency = started.elapsed();
    assert!(
        seek_latency < Duration::from_secs(3),
        "seek took {seek_latency:?}; idle peers weren't woken"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after_seek = downloaded_pieces(&handle);
    assert!(
        after_seek <= after_start + 3,
        "expected only the new window on top, got {after_seek} (was {after_start})"
    );
    assert!(!handle.stats().finished);

    drop(stream);
    session.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_streamed_mode_survives_a_restart() {
    setup_test_logging();
    let seeded = seeded().await;
    let dir = TempDir::with_prefix("test_streamed_mode_restart").unwrap();

    let session = client(&dir, true).await;
    let handle = add_streamed(&session, &seeded.torrent, None).await;
    assert_eq!(handle.download_mode(), DownloadMode::Streamed);
    session.stop().await;
    drop(handle);
    drop(session);

    let restored = client(&dir, true).await;
    let modes = restored.with_torrents(|it| it.map(|(_, t)| t.download_mode()).collect::<Vec<_>>());
    assert_eq!(modes, vec![DownloadMode::Streamed]);
    restored.stop().await;
}
