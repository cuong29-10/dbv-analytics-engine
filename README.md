---
title: DBV Analytics Engine
emoji: 📊
colorFrom: green
colorTo: green
sdk: docker
app_port: 7860
pinned: false
---

# DBV Analytics Engine

Ứng dụng phân tích tỷ lệ bồi thường bảo hiểm vật chất xe cơ giới. Người dùng đăng nhập bằng tài khoản
Microsoft Power BI của chính mình; số liệu được tính trực tiếp trên Microsoft Fabric dưới đúng phân
quyền dữ liệu (RLS) của người đó.

## Biến môi trường cần đặt

| Biến | Ý nghĩa |
|---|---|
| `DBV_BASE_URL` | Địa chỉ công khai của app, ví dụ `https://<user>-<space>.hf.space` |
| `DBV_LOGIN_REQUIRED` | `1` để bắt buộc đăng nhập Microsoft |
| `FABRIC_TENANTID` | Tenant ID của Entra ID |
| `FABRIC_CLIENTID` | Client ID của App Registration |
| `FABRIC_WORKSPACEID` | Workspace ID trên Power BI |
| `FABRIC_DATASETID` | Dataset ID của semantic model |

Redirect URI `<DBV_BASE_URL>/auth/callback` phải được đăng ký trong App Registration, platform
"Mobile and desktop applications".

## Chạy trên máy

```bash
pip install -r requirements.txt
python server.py
```

Mở http://localhost:8787. Cần `fabric_config.json` cạnh `server.py` chứa bốn ID ở trên.
