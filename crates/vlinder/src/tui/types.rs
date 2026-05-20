/// Who produced a transcript entry.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Role {
    User,
    Assistant,
    ToolCall,
}

/// Display data for a rendered tool-call entry.
#[derive(Clone)]
pub struct ToolTraceDisplay {
    pub name: String,
    pub args: String,
    pub result: String,
    pub duration_ms: u64,
    pub is_error: bool,
}

/// One styled entry in the transcript. Multi-line `text` is allowed; the
/// renderer is responsible for handling `\n`.
pub struct Entry {
    pub role: Role,
    pub text: String,
    pub tool: Option<ToolTraceDisplay>,
}
