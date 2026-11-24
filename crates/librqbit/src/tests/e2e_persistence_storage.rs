//! Session persistence decides from the storage's declared capability, not its type:
//! a third-party storage that opts in is persisted and restored, and one that doesn't is
//! refused when it's added.

use std::sync::Arc;

use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions,
    SessionPersistenceConfig, create_torrent,
    spawn_utils::BlockingSpawner,
    storage::{
        BoxStorageFactory, StorageFactory, StorageFactoryExt,
        filesystem::{FilesystemStorage, FilesystemStorageFactory},
    },
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
    torrent_state::{ManagedTorrentShared, TorrentMetadata},
};

/// Stores files like the filesystem storage, but is a different type and keeps the
/// default answer: it hasn't declared that persisting it is sound.
#[derive(Clone)]
struct Undeclared;

impl StorageFactory for Undeclared {
    type Storage = FilesystemStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<FilesystemStorage> {
        FilesystemStorageFactory::default().create(shared, metadata)
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

/// The same storage, declaring that its data survives a restart.
#[derive(Clone)]
struct Declared;

impl StorageFactory for Declared {
    type Storage = FilesystemStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<FilesystemStorage> {
        FilesystemStorageFactory::default().create(shared, metadata)
    }

    fn supports_session_persistence(&self) -> bool {
        true
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

struct Fixture {
    _files: TempDir,
    bytes: bytes::Bytes,
    info_hash: String,
    folder: String,
}

async fn fixture(name: &str) -> Fixture {
    let files = create_default_random_dir_with_torrents(1, 8192, Some(name));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap();
    Fixture {
        bytes: torrent.as_bytes().unwrap(),
        info_hash: torrent.info_hash().as_string(),
        folder: files.path().to_str().unwrap().to_owned(),
        _files: files,
    }
}

async fn session(root: &TempDir) -> Arc<Session> {
    Session::new_with_opts(
        root.path().join("out"),
        SessionOptions {
            dht: None,
            persistence: Some(SessionPersistenceConfig::Json {
                folder: Some(root.path().join("session")),
            }),
            fastresume: true,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn add(
    session: &Arc<Session>,
    fixture: &Fixture,
    storage: Option<BoxStorageFactory>,
) -> anyhow::Result<()> {
    session
        .add_torrent(
            AddTorrent::from_bytes(fixture.bytes.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(fixture.folder.clone()),
                overwrite: true,
                paused: true,
                storage_factory: storage,
                ..Default::default()
            }),
        )
        .await
        .map(|_| ())
}

fn managed(session: &Session) -> Vec<String> {
    let mut hashes = session.with_torrents(|it| {
        it.map(|(_, t)| t.info_hash().as_string())
            .collect::<Vec<_>>()
    });
    hashes.sort();
    hashes
}

#[tokio::test]
async fn test_persistence_follows_storage_capability() {
    setup_test_logging();
    let root = TempDir::with_prefix("test_persistence_storage").unwrap();
    let filesystem = fixture("persist_fs").await;
    let declared = fixture("persist_declared").await;
    let undeclared = fixture("persist_undeclared").await;

    let session1 = session(&root).await;
    add(&session1, &filesystem, None).await.unwrap();
    add(&session1, &declared, Some(Declared.boxed()))
        .await
        .unwrap();
    assert!(
        add(&session1, &undeclared, Some(Undeclared.boxed()))
            .await
            .is_err(),
        "a storage that hasn't declared persistence support must be refused"
    );
    session1.stop().await;
    drop(session1);

    let session2 = session(&root).await;
    let mut expected = vec![filesystem.info_hash.clone(), declared.info_hash.clone()];
    expected.sort();
    assert_eq!(managed(&session2), expected);
    session2.stop().await;
}
