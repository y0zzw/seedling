# MQTT Payloads

- Downlink: V-GNMS to Device(s)
- Uplink: Device(s) to V-GNMS

## Device Polling
```json
// topic: uplink/polling/<DEVICE_ID>
{
    // [string] The ID of the created record (Provided at downlink, must be kept the same)
    "record_id": "0190efb2-d0de-75f7-bcf0-7b0a3e66cda9",

    // [int] Status code you wish to return
    "status": 0,

    // [int] UNIX timestamp in seconds of the responded time
    "responded_at": 1722009318
}
```


## Device Report
```json
// topic: uplink/report/<DEVICE_ID>
{
    // [int] The event type
    "type": 2,

    // [int] The incident level
    "level": 0,

    // [string] Additional custom payload, can be a JSON string
    "info": "Pong",

    // [int] UNIX timestamp in seconds of the report time
    "reported_at": 1722009318
}
```