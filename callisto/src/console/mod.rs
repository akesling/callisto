use std::io;

use std::time::Duration;

use ratatui::{
    backend::CrosstermBackend,
    crossterm::{
        event::{self, KeyCode, KeyEventKind},
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        ExecutableCommand,
    },
    layout,
    widgets::{Block, Borders, Paragraph},
    Terminal,
};

pub fn setup_term_for_console() -> anyhow::Result<()> {
    io::stdout().execute(EnterAlternateScreen)?;
    enable_raw_mode()?;
    Ok(())
}

pub fn teardown_term_for_console() -> anyhow::Result<()> {
    io::stdout().execute(LeaveAlternateScreen)?;
    disable_raw_mode()?;
    Ok(())
}

pub fn run_console<Output>(output: Output) -> anyhow::Result<()>
where
    Output: std::io::Write,
{
    let mut terminal = Terminal::new(CrosstermBackend::new(output))?;
    terminal.clear()?;

    let layout = layout::Layout::default()
        .direction(layout::Direction::Vertical)
        .constraints(vec![
            layout::Constraint::Percentage(20),
            layout::Constraint::Percentage(80),
        ]);

    loop {
        terminal.draw(|frame| {
            let layout = layout.split(frame.size());

            frame.render_widget(
                Paragraph::new("Code console goes here! (press 'q' to quit)")
                    .block(Block::new().borders(Borders::ALL)),
                layout[0],
            );
            frame.render_widget(
                Paragraph::new("Data console goes here!").block(Block::new().borders(Borders::ALL)),
                layout[1],
            );
        })?;

        if event::poll(Duration::from_millis(16))? {
            if let event::Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    Ok(())
}
