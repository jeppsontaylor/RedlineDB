use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use redlinedb_kernel::format::TxId;
use redlinedb_kernel::format::{DEFAULT_PAGE_SIZE, Lsn, Page, PageId, PageKind, RelId, TuplePtr};
use redlinedb_kernel::index::{BtreeIndex, IndexDescriptor, IndexId, IndexRowRef, IndexUniqueness};
use redlinedb_kernel::storage::{BufferPool, PageFile};
use redlinedb_kernel::wal::{WalConfig, WalManager, WalPayload, WalReader, WalRecordKind};

#[test]
fn page_special_area_round_trips() {
    let mut page = Page::new_with_special(
        DEFAULT_PAGE_SIZE,
        PageKind::BtreeLeaf,
        PageId(9),
        RelId(3),
        128,
    )
    .unwrap();
    {
        let special = page.special_bytes_mut().unwrap();
        special[0..4].copy_from_slice(&1234_u32.to_le_bytes());
    }
    page.set_page_lsn(Lsn::new(1)).unwrap();
    let decoded = Page::from_bytes(page.as_bytes().to_vec()).unwrap();
    let special = decoded.special_bytes().unwrap();
    assert_eq!(u32::from_le_bytes(special[0..4].try_into().unwrap()), 1234);
}

#[test]
fn index_insert_lookup() {
    let temp = TempDir::new().unwrap();
    let page_file = Arc::new(PageFile::create(temp.path().join("data.redline"), 512).unwrap());
    let buffer = Arc::new(BufferPool::new(Arc::clone(&page_file), 64).unwrap());
    let index = BtreeIndex::create(
        Arc::clone(&buffer),
        IndexDescriptor::new(IndexId(1), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    index
        .insert(
            b"k001",
            IndexRowRef::new(TuplePtr::new_with_generation(
                PageId(42),
                1,
                redlinedb_kernel::format::PageGeneration::ONE,
            )),
        )
        .unwrap();
    index
        .insert(
            b"k001",
            IndexRowRef::new(TuplePtr::new_with_generation(
                PageId(43),
                2,
                redlinedb_kernel::format::PageGeneration::ONE,
            )),
        )
        .unwrap();
    assert_eq!(index.point_lookup(b"k001").unwrap().len(), 2);
}

#[test]
fn index_split_and_parent_update() {
    let temp = TempDir::new().unwrap();
    let page_file = Arc::new(PageFile::create(temp.path().join("data.redline"), 512).unwrap());
    let buffer = Arc::new(BufferPool::new(Arc::clone(&page_file), 64).unwrap());
    let index = BtreeIndex::create(
        Arc::clone(&buffer),
        IndexDescriptor::new(IndexId(2), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    for i in 0..8_u64 {
        let key = format!("k{i:03}");
        index
            .insert(
                key.as_bytes(),
                IndexRowRef::new(TuplePtr::new_with_generation(
                    PageId(100 + i),
                    i as u16,
                    redlinedb_kernel::format::PageGeneration::ONE,
                )),
            )
            .unwrap();
    }

    for i in 0..8_u64 {
        let key = format!("k{i:03}");
        let hits = index.point_lookup(key.as_bytes()).unwrap();
        assert_eq!(hits.len(), 1);
    }

    let range = index.range_scan(b"k000", b"k999").unwrap();
    assert_eq!(range.len(), 8);
    assert!(index.validate().unwrap().errors.is_empty());
}

#[test]
fn index_recursive_split_propagates_beyond_root() {
    let temp = TempDir::new().unwrap();
    let page_file = Arc::new(PageFile::create(temp.path().join("data.redline"), 512).unwrap());
    let buffer = Arc::new(BufferPool::new(Arc::clone(&page_file), 4096).unwrap());
    let index = BtreeIndex::create(
        Arc::clone(&buffer),
        IndexDescriptor::new(IndexId(5), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    for i in 0..64_u64 {
        let key = format!("k{i:03}");
        index
            .insert(
                key.as_bytes(),
                IndexRowRef::new(TuplePtr::new_with_generation(
                    PageId(300 + i),
                    i as u16,
                    redlinedb_kernel::format::PageGeneration::ONE,
                )),
            )
            .unwrap();
    }

    for i in 0..64_u64 {
        let key = format!("k{i:03}");
        assert_eq!(index.point_lookup(key.as_bytes()).unwrap().len(), 1);
    }

    assert_eq!(index.range_scan(b"k000", b"k999").unwrap().len(), 64);
    let report = index.validate().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(report.internal_pages >= 2);
}

#[test]
fn index_recovery_replays_page_images_with_torn_tail() {
    let source = TempDir::new().unwrap();
    let source_page_file =
        Arc::new(PageFile::create(source.path().join("source.redline"), 384).unwrap());
    let source_buffer = Arc::new(BufferPool::new(Arc::clone(&source_page_file), 64).unwrap());
    let source_index = BtreeIndex::create(
        Arc::clone(&source_buffer),
        IndexDescriptor::new(IndexId(6), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    for i in 0..18_u64 {
        let key = format!("r{i:03}");
        source_index
            .insert(
                key.as_bytes(),
                IndexRowRef::new(TuplePtr::new_with_generation(
                    PageId(500 + i),
                    i as u16,
                    redlinedb_kernel::format::PageGeneration::ONE,
                )),
            )
            .unwrap();
    }
    source_buffer.flush_all(Lsn::ZERO).unwrap();

    let wal_dir = source.path().join("wal");
    let mut wal = WalManager::create(&wal_dir, WalConfig::default()).unwrap();
    for page in snapshot_index_pages(&source_buffer, RelId(1)).unwrap() {
        let payload = WalPayload::PageImage {
            page_id: page.header().unwrap().page_id,
            page_lsn: page.header().unwrap().page_lsn,
            page_bytes: page.as_bytes().to_vec(),
        }
        .encode()
        .unwrap();
        wal.append(WalRecordKind::PageImage, TxId::ZERO, payload)
            .unwrap();
    }
    wal.flush().unwrap();

    let torn = WalPayload::PageImage {
        page_id: PageId(9_999),
        page_lsn: Lsn::new(1),
        page_bytes: vec![1, 2, 3, 4, 5, 6, 7, 8],
    }
    .encode()
    .unwrap();
    let torn_path = wal_dir.join(format!("{:020}.wal", 1_u64));
    let mut torn_file = OpenOptions::new().append(true).open(&torn_path).unwrap();
    torn_file
        .write_all(&torn[..torn.len().saturating_sub(7)])
        .unwrap();

    let mut reader = WalReader::new(&wal_dir, WalConfig::default());
    let scan = reader.scan_report().unwrap();
    assert!(scan.torn_tail);

    let recovered = TempDir::new().unwrap();
    let recovered_page_file =
        Arc::new(PageFile::create(recovered.path().join("recovered.redline"), 384).unwrap());
    let recovered_buffer = Arc::new(BufferPool::new(Arc::clone(&recovered_page_file), 64).unwrap());
    let recovered_index = BtreeIndex::create(
        Arc::clone(&recovered_buffer),
        IndexDescriptor::new(IndexId(6), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    for record in scan.records {
        if let WalPayload::PageImage {
            page_id: _,
            page_lsn: _,
            page_bytes,
        } = WalPayload::decode(&record.payload).unwrap()
        {
            recovered_index
                .redo_page_image(Page::from_bytes(page_bytes).unwrap())
                .unwrap();
        }
    }

    let reopened = BtreeIndex::open(
        Arc::clone(&recovered_buffer),
        PageId(1),
        IndexDescriptor::new(IndexId(6), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();
    assert_eq!(reopened.point_lookup(b"r003").unwrap().len(), 1);
    assert_eq!(reopened.range_scan(b"r000", b"r999").unwrap().len(), 18);
    assert!(reopened.validate().unwrap().errors.is_empty());
}

#[test]
fn index_concurrent_writers_and_readers_stress() {
    let temp = TempDir::new().unwrap();
    let page_file = Arc::new(PageFile::create(temp.path().join("data.redline"), 512).unwrap());
    let buffer = Arc::new(BufferPool::new(Arc::clone(&page_file), 4096).unwrap());
    let index = BtreeIndex::create(
        Arc::clone(&buffer),
        IndexDescriptor::new(IndexId(7), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    let mut writer_handles = Vec::new();
    for writer in 0..4_u64 {
        let index = index.clone();
        writer_handles.push(thread::spawn(move || {
            for i in 0..16_u64 {
                let key = format!("w{writer}_{i:03}");
                index
                    .insert(
                        key.as_bytes(),
                        IndexRowRef::new(TuplePtr::new_with_generation(
                            PageId(800 + writer * 100 + i),
                            i as u16,
                            redlinedb_kernel::format::PageGeneration::ONE,
                        )),
                    )
                    .unwrap();
            }
        }));
    }

    for handle in writer_handles {
        handle.join().unwrap();
    }

    for writer in 0..4_u64 {
        for i in 0..16_u64 {
            let key = format!("w{writer}_{i:03}");
            assert_eq!(index.point_lookup(key.as_bytes()).unwrap().len(), 1);
        }
    }
    assert_eq!(index.range_scan(b"w0_000", b"w9_999").unwrap().len(), 64);
    assert!(index.validate().unwrap().errors.is_empty());
}

#[test]
fn index_redo_page_image_restores_corruption() {
    let temp = TempDir::new().unwrap();
    let page_file = Arc::new(PageFile::create(temp.path().join("data.redline"), 512).unwrap());
    let buffer = Arc::new(BufferPool::new(Arc::clone(&page_file), 64).unwrap());
    let index = BtreeIndex::create(
        Arc::clone(&buffer),
        IndexDescriptor::new(IndexId(3), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    index
        .insert(
            b"k001",
            IndexRowRef::new(TuplePtr::new_with_generation(
                PageId(77),
                1,
                redlinedb_kernel::format::PageGeneration::ONE,
            )),
        )
        .unwrap();

    let snapshot = buffer
        .pin(PageId(2))
        .unwrap()
        .with_page(|page| Ok(page.clone()))
        .unwrap();

    buffer
        .pin(PageId(2))
        .unwrap()
        .with_page_mut(|page| {
            page.as_mut_bytes_for_io_test()[128] ^= 0xFF;
            Ok(())
        })
        .unwrap();

    index.redo_page_image(snapshot).unwrap();
    assert_eq!(index.point_lookup(b"k001").unwrap().len(), 1);
}

#[test]
fn unique_lock_waits_and_conflict_checks() {
    let temp = TempDir::new().unwrap();
    let page_file = Arc::new(PageFile::create(temp.path().join("data.redline"), 512).unwrap());
    let buffer = Arc::new(BufferPool::new(Arc::clone(&page_file), 64).unwrap());
    let index = BtreeIndex::create(
        Arc::clone(&buffer),
        IndexDescriptor::new(IndexId(4), RelId(1), IndexUniqueness::Unique),
    )
    .unwrap();

    let lock_guard = index.lock_unique_key(1, b"k001").unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let index_clone = index.clone();
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = index_clone.insert_unique(
            2,
            b"k001",
            IndexRowRef::new(TuplePtr::new_with_generation(
                PageId(88),
                1,
                redlinedb_kernel::format::PageGeneration::ONE,
            )),
        );
        done_tx.send(result).unwrap();
    });

    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    drop(lock_guard);

    let result = done_rx.recv().unwrap();
    assert!(result.is_ok());
    handle.join().unwrap();

    index
        .insert_unique(
            1,
            b"k002",
            IndexRowRef::new(TuplePtr::new_with_generation(
                PageId(89),
                2,
                redlinedb_kernel::format::PageGeneration::ONE,
            )),
        )
        .unwrap();
    let duplicate = index.insert_unique(
        3,
        b"k002",
        IndexRowRef::new(TuplePtr::new_with_generation(
            PageId(90),
            3,
            redlinedb_kernel::format::PageGeneration::ONE,
        )),
    );
    assert_eq!(
        duplicate.unwrap_err(),
        redlinedb_kernel::Error::WriteConflict
    );
}

#[test]
fn index_delete_mark_and_compact_leaf() {
    let temp = TempDir::new().unwrap();
    let page_file = Arc::new(PageFile::create(temp.path().join("data.redline"), 512).unwrap());
    let buffer = Arc::new(BufferPool::new(Arc::clone(&page_file), 64).unwrap());
    let index = BtreeIndex::create(
        Arc::clone(&buffer),
        IndexDescriptor::new(IndexId(8), RelId(1), IndexUniqueness::NonUnique),
    )
    .unwrap();

    let row1 = IndexRowRef::new(TuplePtr::new_with_generation(
        PageId(200),
        1,
        redlinedb_kernel::format::PageGeneration::ONE,
    ));
    let row2 = IndexRowRef::new(TuplePtr::new_with_generation(
        PageId(201),
        2,
        redlinedb_kernel::format::PageGeneration::ONE,
    ));
    index.insert(b"k001", row1).unwrap();
    index.insert(b"k001", row2).unwrap();
    assert_eq!(index.point_lookup(b"k001").unwrap().len(), 2);

    index.delete_mark(b"k001", row1).unwrap();
    assert_eq!(index.point_lookup(b"k001").unwrap().len(), 1);
    index.compact_leaf_page(PageId(2)).unwrap();
    assert_eq!(index.point_lookup(b"k001").unwrap().len(), 1);

    index.delete_mark(b"k001", row2).unwrap();
    assert_eq!(index.point_lookup(b"k001").unwrap().len(), 0);
    index.compact_leaf_page(PageId(2)).unwrap();
    assert!(index.validate().unwrap().errors.is_empty());
}

fn snapshot_index_pages(
    buffer: &Arc<BufferPool>,
    rel_id: RelId,
) -> redlinedb_kernel::Result<Vec<Page>> {
    let mut pages = Vec::new();
    let page_count = buffer.page_count()?;
    for page_no in 1..=page_count {
        let page_id = PageId(page_no);
        if let Ok(guard) = buffer.pin(page_id) {
            let page = guard.with_page(|page| {
                let header = page.header()?;
                if header.rel_id == rel_id
                    && matches!(
                        header.kind,
                        PageKind::BtreeMeta | PageKind::BtreeLeaf | PageKind::BtreeInternal
                    )
                {
                    Ok(Some(page.clone()))
                } else {
                    Ok(None)
                }
            })?;
            if let Some(page) = page {
                pages.push(page);
            }
        }
    }
    Ok(pages)
}
