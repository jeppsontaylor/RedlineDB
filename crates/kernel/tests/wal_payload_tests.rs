use redlinedb_kernel::Error;
use redlinedb_kernel::format::{Csn, RelId, RowId, TxId};
use redlinedb_kernel::wal::WalPayload;

#[test]
fn wal_payload_variants_round_trip() {
    let payloads = [
        WalPayload::HeapInsert {
            tx_id: TxId(1),
            rel_id: RelId(11),
            row_id: RowId(2),
            payload: b"insert".to_vec(),
        },
        WalPayload::HeapUpdate {
            tx_id: TxId(3),
            rel_id: RelId(11),
            row_id: RowId(4),
            payload: b"update".to_vec(),
        },
        WalPayload::HeapDelete {
            tx_id: TxId(5),
            rel_id: RelId(11),
            row_id: RowId(6),
        },
        WalPayload::Commit {
            tx_id: TxId(7),
            csn: Csn(8),
        },
    ];

    for payload in payloads {
        let encoded = payload.encode().unwrap();
        let decoded = WalPayload::decode(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }
}

#[test]
fn wal_payload_rejects_truncated_payload() {
    let payload = WalPayload::HeapInsert {
        tx_id: TxId(1),
        rel_id: RelId(11),
        row_id: RowId(2),
        payload: b"insert".to_vec(),
    };
    let mut encoded = payload.encode().unwrap();
    encoded.pop();

    let err = WalPayload::decode(&encoded).unwrap_err();
    assert!(matches!(err, Error::BufferTooSmall { .. }));
}

#[test]
fn wal_payload_rejects_unknown_tag() {
    let err = WalPayload::decode(&[99]).unwrap_err();
    assert_eq!(err, Error::CorruptWal("unknown wal payload tag"));
}
