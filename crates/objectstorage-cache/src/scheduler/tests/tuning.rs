//! Scheduler tests.

use super::*;

#[tokio::test]
async fn slots_follow_the_setting_up_at_once_down_as_fetches_finish() {
    let tuning = Arc::new(Tuning::new(2, 0));
    let s = Scheduler::new("app", "test", tuning.clone());
    assert_eq!(s.slots.available_permits(), 2);
    tuning.set(5, 1);
    s.resize();
    assert_eq!(s.slots.available_permits(), 5);
    // Down to 1 while 2 are in use: the surplus goes as slots free up.
    let held: Vec<_> = (0..2)
        .map(|_| s.slots.clone().try_acquire_owned().unwrap())
        .collect();
    tuning.set(1, 0);
    s.resize();
    tokio::task::yield_now().await;
    drop(held);
    for _ in 0..50 {
        if s.slots.available_permits() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(s.slots.available_permits(), 1);
}
