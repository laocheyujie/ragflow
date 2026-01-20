const jsrsasign = require('jsrsasign');
const CryptoJS = require('crypto-js');

// ================== 配置区 ==================
const VAR_NAME_INPUT = "PASSWORD";
const VAR_NAME_OUTPUT = "password";

// 公钥 (原样保留 PEM 格式)
const publicKeyPem = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArq9XTUSeYr2+N1h3Afl/
z8Dse/2yD0ZGrKwx+EEEcdsBLca9Ynmx3nIB5obmLlSfmskLpBo0UACBmB5rEjBp
2Q2f3AG3Hjd4B+gNCG6BDaawuDlgANIhGnaTLrIqWrrcm4EMzJOnAOI1fgzJRsOO
UEfaS318Eq9OVO3apEyCCt0lOQK6PuksduOjVxtltDav+guVAA068NrPYmRNabVK
RNLJpL8w4D44sfth5RvZ3q9t+6RTArpEtc5sh5ChzvqPOzKGMXW83C95TxmXqpbK
6olN4RevSfVjEAgCydH6HN6OhtOQEcnrU97r9H0iZOWwbw3pVrZiUkuRD1R56Wzs
2wIDAQAB
-----END PUBLIC KEY-----`;

// ================== 逻辑区 ==================

// 1. 严格获取环境变量，不存在则抛出异常终止请求
let plainPassword = pm.variables.get(VAR_NAME_INPUT);

if (!plainPassword) {
    const errorMsg = `⛔️ 致命错误：未找到环境变量【${VAR_NAME_INPUT}】！\n请在“环境变量”或接口“运行参数”中设置该变量。`;
    console.error(errorMsg);
    throw new Error(errorMsg); // 这会让 Apifox 停止发送后续请求
}

console.log(`正在加密变量 ${VAR_NAME_INPUT}...`);

try {
    // 2. 第一层处理：明文转 Base64 (Python: base64.b64encode)
    // 使用 CryptoJS 处理 UTF-8 字符（如中文昵称或密码）
    const step1_wordArray = CryptoJS.enc.Utf8.parse(plainPassword);
    const step1_base64 = CryptoJS.enc.Base64.stringify(step1_wordArray);

    // 3. 第二层处理：RSA 加密 (Python: Cipher_pkcs1_v1_5.encrypt)
    // 加载公钥
    const pubKeyObj = jsrsasign.KEYUTIL.getKey(publicKeyPem);
    
    // 加密：输入是上面的 Base64 字符串
    // jsrsasign 的 "RSA" 算法对应 PKCS#1 v1.5 padding
    const encryptedHex = jsrsasign.KJUR.crypto.Cipher.encrypt(step1_base64, pubKeyObj, "RSA");

    // 4. 第三层处理：密文转 Base64 (Python: base64.b64encode)
    // jsrsasign 输出是 Hex，需要转回 Base64
    const finalPassword = jsrsasign.hextob64(encryptedHex);

    // 5. 设置结果到环境变量
    pm.variables.set(VAR_NAME_OUTPUT, finalPassword);
    
    console.log("✅ 加密成功，已自动填充 {{password}}");

} catch (e) {
    console.error("❌ 加密过程发生异常:", e);
    throw e; // 抛出异常以终止请求
}