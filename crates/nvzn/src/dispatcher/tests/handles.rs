use super::*;

#[test]
fn encoder_prepends_tenant_id() {
    let events = [event::Event::Pinged(testsupport::PingedV1 {
        id: UserId(1),
        ts: 0,
    })];
    let encoder = Encoder::new(tenant());
    let bytes = encoder.encode(&events).unwrap();
    assert_eq!(&bytes[..16], &tenant_id().as_slice());
    let decoded = event::Event::decode(&bytes[16..]).unwrap();
    assert_eq!(decoded.len(), 1);
}
