pub struct MailboxConfig {
    pub event_buffer_size: usize,
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 100,
        }
    }
}
