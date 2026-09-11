// Đăng nhập Microsoft và gọi Power BI trực tiếp từ trình duyệt.
// api.powerbi.com bật CORS cho mọi origin, nên không cần server trung gian; RLS vẫn do Power BI áp
// theo đúng tài khoản đăng nhập, y như khi người đó mở báo cáo.

const CFG = window.DBV_CONFIG;
const SCOPES = ["https://analysis.windows.net/powerbi/api/.default"];

let msalApp = null;
let account = null;

export async function initAuth() {
  msalApp = new msal.PublicClientApplication({
    auth: {
      clientId: CFG.clientId,
      authority: `https://login.microsoftonline.com/${CFG.tenantId}`,
      redirectUri: window.location.origin + window.location.pathname,
    },
    cache: { cacheLocation: "localStorage" },
  });
  await msalApp.initialize();
  const result = await msalApp.handleRedirectPromise();
  account = result?.account || msalApp.getAllAccounts()[0] || null;
  if (account) msalApp.setActiveAccount(account);
  return account;
}

export function login() {
  return msalApp.loginRedirect({ scopes: SCOPES });
}

export function logout() {
  return msalApp.logoutRedirect({ account });
}

async function getToken() {
  if (!account) throw new Error("Chưa đăng nhập Microsoft.");
  try {
    const r = await msalApp.acquireTokenSilent({ scopes: SCOPES, account });
    return r.accessToken;
  } catch {
    // Phiên hết hạn hoặc cần xác thực lại: đưa người dùng về trang Microsoft, quay lại là chạy tiếp.
    await msalApp.acquireTokenRedirect({ scopes: SCOPES, account });
    throw new Error("Đang chuyển sang trang đăng nhập Microsoft…");
  }
}

const API = "https://api.powerbi.com/v1.0/myorg";

// Lỗi tạm thời (429 quá tải, 5xx service) giữa nhiều lượt gọi liên tiếp là chuyện bình thường; lỗi DAX
// thật (400, sai cú pháp hoặc tên cột) thì báo ngay, thử lại vô ích.
export async function runDax(query, retries = 4) {
  const token = await getToken();
  const url = `${API}/groups/${CFG.workspaceId}/datasets/${CFG.datasetId}/executeQueries`;
  const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
  let lastErr = null;
  for (let attempt = 0; attempt < retries; attempt++) {
    let res;
    try {
      res = await fetch(url, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body,
      });
    } catch (e) {
      lastErr = e;
      await sleep(2 ** attempt * 1000);
      continue;
    }
    if (res.status === 429 || res.status >= 500) {
      lastErr = new Error(`Power BI báo lỗi tạm thời HTTP ${res.status}`);
      await sleep(Math.min(30000, (2 ** attempt + 1) * 1000));
      continue;
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const msg = data?.error?.["pbi.error"]?.details?.[0]?.detail?.value || data?.error?.message || `HTTP ${res.status}`;
      throw new Error(`Power BI lỗi: ${msg}`);
    }
    const err = data?.results?.[0]?.error;
    if (err) throw new Error(`DAX lỗi: ${err.message || JSON.stringify(err)}`);
    return data.results[0].tables[0].rows;
  }
  throw new Error(`Power BI lỗi liên tục sau ${retries} lần thử: ${lastErr?.message || lastErr}`);
}

// Thời điểm dataset hoàn tất làm mới lần gần nhất — mốc "số liệu tới lúc nào" của mọi truy vấn.
export async function lastRefresh() {
  const token = await getToken();
  const url = `${API}/groups/${CFG.workspaceId}/datasets/${CFG.datasetId}/refreshes?$top=10`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) return null;
  const data = await res.json();
  const done = (data.value || []).find((x) => x.status === "Completed" && x.endTime);
  return done ? done.endTime : null;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
