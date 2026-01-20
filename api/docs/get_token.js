/**
 * RAGFlow 自动鉴权脚本 (放在根目录前置操作)
 * 功能：
 * 1. 自动识别是否需要登录（排除登录接口本身，检查 Token 有效期）
 * 2. 密码 RSA 加密
 * 3. 获取 Token 并写入环境变量 TOKEN
 */

// ================= 配置区 =================
const CONFIG = {
    // 登录接口路径
    loginPath: "/user/login",
    // 环境变量 Key 配置
    envKeys: {
        email: "EMAIL",           // 输入：邮箱
        password: "PASSWORD",     // 输入：明文密码
        token: "TOKEN",           // 输出：Token
        tokenTime: "TOKEN_TIME"   // 输出：Token获取时间戳
    },
    // Token 有效期 (分钟)，小于此时间不重新登录
    expireMinutes: 1440
};

// 公钥 (RAGFlow 前端同款)
const PUBLIC_KEY = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArq9XTUSeYr2+N1h3Afl/
z8Dse/2yD0ZGrKwx+EEEcdsBLca9Ynmx3nIB5obmLlSfmskLpBo0UACBmB5rEjBp
2Q2f3AG3Hjd4B+gNCG6BDaawuDlgANIhGnaTLrIqWrrcm4EMzJOnAOI1fgzJRsOO
UEfaS318Eq9OVO3apEyCCt0lOQK6PuksduOjVxtltDav+guVAA068NrPYmRNabVK
RNLJpL8w4D44sfth5RvZ3q9t+6RTArpEtc5sh5ChzvqPOzKGMXW83C95TxmXqpbK
6olN4RevSfVjEAgCydH6HN6OhtOQEcnrU97r9H0iZOWwbw3pVrZiUkuRD1R56Wzs
2wIDAQAB
-----END PUBLIC KEY-----`;

// ================= 逻辑区 =================

(function main() {
    // 1. 【防死循环】如果当前就在运行登录接口，直接退出
    const currentUrl = pm.request.url.toString();
    if (currentUrl.includes(CONFIG.loginPath)) {
        console.log("🚫 当前是登录接口，跳过自动鉴权脚本。");
        return;
    }

    // 2. 【缓存检查】检查 Token 是否仍在有效期内
    const lastLoginTime = pm.environment.get(CONFIG.envKeys.tokenTime);
    const currentToken = pm.environment.get(CONFIG.envKeys.token);
    
    if (currentToken && lastLoginTime) {
        const elapsedMinutes = (Date.now() - parseInt(lastLoginTime)) / 1000 / 60;
        if (elapsedMinutes < CONFIG.expireMinutes) {
            console.log(`✅ Token 有效 (已用 ${elapsedMinutes.toFixed(1)}/${CONFIG.expireMinutes} 分钟)，跳过登录。`);
            return;
        }
        console.log(`⚠️ Token 已过期 (已用 ${elapsedMinutes.toFixed(1)} 分钟)，正在重新获取...`);
    } else {
        console.log("⚠️ 未找到有效 Token，准备首次登录...");
    }

    // 3. 准备登录数据
    const baseUrl = pm.environment.get("BASE_URL");
    if (!baseUrl) {
        console.error("❌ 致命错误：环境变量 'BASE_URL' 未设置！");
        return;
    }
    
    const email = pm.environment.get(CONFIG.envKeys.email);
    const rawPassword = pm.environment.get(CONFIG.envKeys.password);

    if (!email || !rawPassword) {
        console.error(`❌ 致命错误：请设置环境变量 ${CONFIG.envKeys.email} 和 ${CONFIG.envKeys.password}`);
        return;
    }

    // 4. 执行加密与请求
    try {
        const loginUrl = baseUrl.replace(/\/+$/, "") + CONFIG.loginPath;
        const encryptedPassword = encryptPassword(rawPassword);

        const loginReq = {
            url: loginUrl,
            method: 'POST',
            header: { 'Content-Type': 'application/json' },
            body: {
                mode: 'raw',
                raw: JSON.stringify({ email: email, password: encryptedPassword })
            }
        };

        pm.sendRequest(loginReq, (err, res) => {
            if (err) {
                console.error("❌ 登录请求网络错误:", err);
                return;
            }
            if (res.code !== 200) {
                console.error(`❌ 登录失败 [${res.code}]:`, res.text());
                return;
            }

            // 5. 提取并保存 Token
            const token = extractAuthHeader(res.headers);
            if (token) {
                pm.environment.set(CONFIG.envKeys.token, token);
                pm.environment.set(CONFIG.envKeys.tokenTime, Date.now());
                console.log("✅ 自动登录成功，环境变量已更新。");
            } else {
                console.error("❌ 登录响应中未找到 Authorization 头");
            }
        });

    } catch (e) {
        console.error("❌ 脚本执行异常:", e);
    }
})();

// ================= 工具函数 =================

function encryptPassword(pwd) {
    const jsrsasign = require('jsrsasign');
    const CryptoJS = require('crypto-js');
    
    // Base64 -> RSA -> Base64
    const step1 = CryptoJS.enc.Base64.stringify(CryptoJS.enc.Utf8.parse(pwd));
    const pubKeyObj = jsrsasign.KEYUTIL.getKey(PUBLIC_KEY);
    const encryptedHex = jsrsasign.KJUR.crypto.Cipher.encrypt(step1, pubKeyObj, "RSA");
    return jsrsasign.hextob64(encryptedHex);
}

function extractAuthHeader(headers) {
    // 兼容处理：headers 可能是数组(旧版Postman)或对象(Apifox/新版Postman)或Map
    if (headers.get && typeof headers.get === 'function') {
        return headers.get("Authorization");
    }
    if (Array.isArray(headers)) {
        const h = headers.find(item => item.key.toLowerCase() === "authorization");
        return h ? h.value : null;
    }
    // 尝试直接对象访问
    return headers["Authorization"] || headers["authorization"];
}