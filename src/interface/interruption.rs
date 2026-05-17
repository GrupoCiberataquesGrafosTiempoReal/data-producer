use anyhow::Result;
use crossterm::{
    event::{poll, read, Event, KeyCode, KeyEvent},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use std::time::Duration;

fn enable_keyboard_capture() -> Result<()> {
    enable_raw_mode()?;
    Ok(())
}

fn disable_keyboard_capture() -> Result<()> {
    disable_raw_mode()?;
    Ok(())
}

pub fn interruption_requested() -> Result<bool> {
    enable_keyboard_capture()?;
    if poll(Duration::from_millis(10))? {
        if let Event::Key(KeyEvent {
            code: KeyCode::Char('S'),
            ..
        }) = read()?
        {
            println!("\nStopped by user (S)");
            disable_keyboard_capture()?;
            return Ok(true);
        }
    }

    disable_keyboard_capture()?;
    Ok(false)
}