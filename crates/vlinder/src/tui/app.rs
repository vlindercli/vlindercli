//! UI state. Pure data plus methods that enforce state invariants
//! (notably the scroll-offset / follow-tail interaction). Knows nothing
//! about rendering, theming, or events.

use ratatui_textarea::TextArea;

use super::theme;

/// Who produced a transcript entry.
#[derive(Clone, Copy)]
pub enum Role {
    User,
    Assistant,
}

/// One styled entry in the transcript. Multi-line `text` is allowed; the
/// renderer is responsible for handling `\n`.
pub struct Entry {
    pub role: Role,
    pub text: String,
}

/// All UI state for the REPL. Public fields are read by renderers; mutation
/// goes through methods to keep the scroll invariant honest.
pub struct App {
    /// Conversation transcript, oldest first.
    pub output: Vec<Entry>,

    /// The input editor. Holds only the current draft.
    pub textarea: TextArea<'static>,

    /// Whether a request is in flight.
    pub spinning: bool,

    /// Index into the spinner frame array, advanced by the animation loop.
    pub spinner_frame: usize,

    /// Current scroll position, measured in visual rows after wrapping.
    /// Should only be mutated via the `scroll_*` / `clamp_scroll` methods.
    pub scroll_offset: u16,

    /// When true, the transcript stays pinned to the latest line and new
    /// output auto-scrolls into view. Scrolling up disengages this; reaching
    /// the bottom (or jumping there) re-engages it.
    pub follow_tail: bool,
}

impl App {
    pub fn new() -> Self {
        let mut textarea = TextArea::default();
        theme::configure_input(&mut textarea);
        Self {
            output: Vec::new(),
            textarea,
            spinning: false,
            spinner_frame: 0,
            scroll_offset: 0,
            follow_tail: true,
        }
    }

    // ── Transcript ───────────────────────────────────────────────────────

    pub fn push_user(&mut self, text: impl Into<String>) {
        self.output.push(Entry {
            role: Role::User,
            text: text.into(),
        });
    }

    pub fn push_assistant(&mut self, text: impl Into<String>) {
        self.output.push(Entry {
            role: Role::Assistant,
            text: text.into(),
        });
    }

    // ── Input ────────────────────────────────────────────────────────────

    /// Reset the input buffer to a single empty line.
    pub fn clear_input(&mut self) {
        self.textarea = TextArea::default();
        theme::configure_input(&mut self.textarea);
    }

    // ── Scroll invariant ─────────────────────────────────────────────────
    //
    // The contract: `follow_tail = true` means "new output auto-scrolls".
    // Scrolling up disengages it; scrolling down to the bottom re-engages
    // it (handled in `clamp_scroll` since we only know "the bottom" at
    // render time).

    /// Scroll up by `n` rows. Disengages tail-following.
    pub fn scroll_up(&mut self, n: u16) {
        self.follow_tail = false;
        self.scroll_offset = self.scroll_offset.saturating_sub(n);
    }

    /// Scroll down by `n` rows. `clamp_scroll` may re-engage tail-following
    /// if this lands us at the bottom.
    pub fn scroll_down(&mut self, n: u16) {
        self.scroll_offset = self.scroll_offset.saturating_add(n);
    }

    /// Jump to the top. Disengages tail-following.
    pub fn jump_top(&mut self) {
        self.follow_tail = false;
        self.scroll_offset = 0;
    }

    /// Jump to the live tail. The next render will set `scroll_offset`.
    pub fn jump_bottom(&mut self) {
        self.follow_tail = true;
    }

    /// Reconcile `scroll_offset` against the maximum scroll position. Called
    /// from the renderer once it knows the wrapped row count. Re-engages
    /// tail-following if the user has scrolled all the way down.
    pub fn clamp_scroll(&mut self, max_scroll: u16) {
        if self.follow_tail {
            self.scroll_offset = max_scroll;
        } else {
            let clamped = self.scroll_offset.min(max_scroll);
            if clamped == max_scroll {
                self.follow_tail = true;
            }
            self.scroll_offset = clamped;
        }
    }
}
