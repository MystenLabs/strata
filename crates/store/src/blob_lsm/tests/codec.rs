//! Round-trip tests for the wire formats in `format`.

use std::collections::BTreeMap;

use core_types::BlobLifecycle;

use crate::blob_lsm::format::BlobMutationWithLSN;
use crate::blob_lsm::{BlobLifetime, BlobMutation, BlobState, BlobVersion};

use super::{record, shard};

#[test]
fn mutation_codecs_round_trip() {
    let record_ref = record(7, 8);
    let put = BlobMutation::decode_put_metadata(
        &BlobMutation::encode_put_metadata(shard(1, 2), 3),
        record_ref,
    )
    .unwrap();
    assert_eq!(
        put,
        BlobMutation::Put {
            shard: shard(1, 2),
            write_epoch: 3,
            record_ref,
        }
    );
    assert!(put.encode_inline().is_err());

    let inline_mutations = [
        BlobMutation::SetLifetime {
            logical_end_epoch: 9,
            current_epoch: 4,
        },
        BlobMutation::Tombstone { shard: shard(5, 6) },
        BlobMutation::Relocate {
            shard: shard(5, 6),
            payload_lsn: 7,
            to: record(9, 11),
        },
    ];
    for mutation in inline_mutations {
        assert_eq!(
            BlobMutationWithLSN::decode_inline(10, &mutation.encode_inline().unwrap()).unwrap(),
            vec![BlobMutationWithLSN { lsn: 10, mutation }]
        );
    }

    let batch = vec![
        BlobMutationWithLSN {
            lsn: 3,
            mutation: put,
        },
        BlobMutationWithLSN {
            lsn: 4,
            mutation: BlobMutation::Relocate {
                shard: shard(1, 2),
                payload_lsn: 3,
                to: record(9, 11),
            },
        },
    ];
    assert_eq!(
        BlobMutationWithLSN::decode_inline(4, &BlobMutationWithLSN::encode_batch(&batch).unwrap())
            .unwrap(),
        batch
    );
}

#[test]
fn materialized_state_codec_round_trips() {
    let state = BlobState {
        versions: BTreeMap::from([(
            shard(1, 2),
            BlobVersion {
                lsn: 3,
                write_epoch: 5,
                record_ref: record(6, 7),
            },
        )]),
        lifetime: Some(BlobLifetime {
            lsn: 8,
            lifecycle: BlobLifecycle {
                logical_end_epoch: 10,
                extension_count: 11,
            },
        }),
    };
    assert_eq!(BlobState::decode(&state.encode().unwrap()).unwrap(), state);
}
