def test_health_check(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "user-service"
    assert body["checks"]["database"] == "ok"
