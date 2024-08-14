use std::fmt::Display;

use crate::ast::{BinOpKind, UnaryOpKind};

#[derive(Debug, PartialEq, Copy, Clone, Default, Eq)]
pub(crate) enum Op {
    /// do absolutely nothing
    #[default]
    Nop,
    /// take the top most value from the stack and delete it
    Pop,
    /// swap the top two values on the stack
    Swap,
    /// duplicate the top of the stack
    #[allow(dead_code)]
    Duplicate,
    /// Load V1, pops the stack and stores the value in V1
    LoadV1,
    /// Stores the value in V1 on the stack and sets it to null
    StoreV1,
    /// Swaps the value in V1 with the top of the stack
    SwapV1,
    /// Copies the content of V1 to the top of the stack
    CopyV1,
    /// Load boolean register from the top of the stack
    LoadRB,
    /// Store boolean register to the top of the stack
    #[allow(dead_code)]
    StoreRB,
    /// Puts the event on the stack
    LoadEvent,
    /// Takes the top of the stack and stores it in the event
    StoreEvent {
        elements: u16,
    },
    /// puts a variable on the stack
    LoadLocal {
        idx: u32,
    },
    /// stores a variable from the stack
    StoreLocal {
        elements: u16,
        idx: u32,
    },
    /// emits an error
    Error,
    /// emits the top of the stack
    Emit {
        dflt: bool,
    },
    /// drops the event
    Drop,
    /// jumps to the given offset if the top of the stack is true does not op the stack
    JumpTrue {
        dst: u32,
    },
    /// jumps to the given offset if the top of the stack is true does not op the stack
    JumpFalse {
        dst: u32,
    },
    /// jumps to the given offset if the top of the stack is true does not op the stack
    Jump {
        dst: u32,
    },
    Const {
        idx: u32,
    },

    // Values
    #[allow(dead_code)]
    True,
    #[allow(dead_code)]
    False,
    Null,
    Record {
        size: u32,
    },
    Array {
        size: u32,
    },
    String {
        size: u32,
    },
    Bytes {
        size: u32,
    },
    // Logical XOP
    Xor,

    Binary {
        op: BinOpKind,
    },
    Unary {
        op: UnaryOpKind,
    },

    GetKey {
        key: u32,
    },
    Get,
    Index,
    IndexFast {
        idx: u32,
    },
    Range,
    RangeFast {
        start: u16,
        end: u16,
    },

    // Tests - does not pop the stack result is stored in the b register
    TestRecortPresent,
    TestIsU64,
    TestIsI64,
    TestIsBytes,
    #[allow(dead_code)]
    TestEq,
    #[allow(dead_code)]
    TestNeq,
    TestGt,
    #[allow(dead_code)]
    TestGte,
    TestLt,
    #[allow(dead_code)]
    TestLte,

    #[allow(dead_code)]
    TestArrayIsEmpty,
    TestRecordIsEmpty,

    // Inspect - does not pop the stack result is stored on the stack
    //// returns the lenght of an array, object or 1 for scalar values
    InspectLen,

    // Patch
    RecordSet,
    RecordRemove,
    RecordGet,
    // Merge
    RecordMerge,
    TestIsRecord,
    TestIsArray,
    RecordMergeKey,
    RecordPop,
    ArrayPop,
    ArrayReverse,
}

impl Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Nop => write!(f, "nop"),
            Op::Pop => write!(f, "pop"),
            Op::Swap => write!(f, "swap"),
            Op::Duplicate => write!(f, "duplicate"),
            Op::Error => write!(f, "error"),

            Op::LoadV1 => write!(f, "{:30} V1", "load_reg"),
            Op::StoreV1 => write!(f, "{:30} V1", "store_reg"),
            Op::SwapV1 => write!(f, "{:30} V1", "swap_reg"),
            Op::CopyV1 => write!(f, "{:30} V1", "copy_reg"),

            Op::LoadRB => write!(f, "{:30} B1", "load_reg"),
            Op::StoreRB => write!(f, "{:30} B1", "store_reg"),

            Op::LoadEvent => write!(f, "laod_event"),
            Op::StoreEvent { elements } => write!(f, "{:30} {elements:<5}", "store_event"),
            Op::LoadLocal { idx } => write!(f, "{:30} {idx:<5}", "load_local",),
            Op::StoreLocal { elements, idx } => {
                write!(f, "{:30} {idx:<5} {elements}", "store_local")
            }

            Op::Emit { dflt } => write!(f, "{:30} {dflt:<5}", "emit"),
            Op::Drop => write!(f, "drop"),
            Op::JumpTrue { dst } => write!(f, "{:30} {:<5}", "jump_true", dst),
            Op::JumpFalse { dst } => write!(f, "{:30} {:<5}", "jump_false", dst),
            Op::Jump { dst } => write!(f, "{:30} {:<5}", "jump", dst),
            Op::True => write!(f, "true"),
            Op::False => write!(f, "false"),
            Op::Null => write!(f, "null"),
            Op::Const { idx } => write!(f, "{:30} {idx:<5}", "const"),
            Op::Record { size } => write!(f, "{:30} {size:<5}", "record",),
            Op::Array { size } => write!(f, "{:30} {size:<5}", "array",),
            Op::String { size } => write!(f, "{:30} {size:<5}", "string",),
            Op::Bytes { size } => write!(f, "{:30} {size:<5}", "bytes",),
            Op::Xor => write!(f, "xor"),
            Op::Binary { op } => write!(f, "{:30} {:<5?}", "binary", op),
            Op::Unary { op } => write!(f, "{:30} {:<5?}", "unary", op),
            Op::GetKey { key } => write!(f, "{:30} {}", "lookup_key", key),
            Op::Get => write!(f, "lookup"),
            Op::Index => write!(f, "idx"),
            Op::IndexFast { idx } => write!(f, "{:30} {:<5}", "idx_fast", idx),
            Op::Range => write!(f, "range"),
            Op::RangeFast { start, end } => write!(f, "{:30} {:<5} {}", "range_fast", start, end),

            Op::TestRecortPresent => write!(f, "test_record_present"),
            Op::TestIsU64 => write!(f, "test_is_u64"),
            Op::TestIsI64 => write!(f, "test_is_i64"),
            Op::TestIsBytes => write!(f, "test_is_bytes"),
            Op::TestIsRecord => write!(f, "test_is_record"),
            Op::TestIsArray => write!(f, "test_is_array"),
            Op::TestArrayIsEmpty => write!(f, "test_array_is_empty"),
            Op::TestRecordIsEmpty => write!(f, "test_record_is_empty"),
            Op::TestEq => write!(f, "test_eq"),
            Op::TestNeq => write!(f, "test_neq"),
            Op::TestGt => write!(f, "test_gt"),
            Op::TestGte => write!(f, "test_gte"),
            Op::TestLt => write!(f, "test_lt"),
            Op::TestLte => write!(f, "test_lte"),

            Op::InspectLen => write!(f, "inspect_len"),

            Op::RecordSet => write!(f, "record_set"),
            Op::RecordRemove => write!(f, "record_remove"),
            Op::RecordGet => write!(f, "record_get"),
            Op::RecordMergeKey => write!(f, "record_merge_key"),
            Op::RecordMerge => write!(f, "record_merge"),

            Op::RecordPop => write!(f, "record_pop"),
            Op::ArrayPop => write!(f, "array_pop"),
            Op::ArrayReverse => write!(f, "array_reverse"),
        }
    }
}
