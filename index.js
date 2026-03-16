import dotenv from "dotenv";
import express from "express";
import Airtable from "airtable";
import QRCode from "qrcode";
import puppeteer from "puppeteer";
import { PDFDocument } from "pdf-lib";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

dotenv.config();

const app = express();
app.use(express.json({ limit: "2mb" }));

/* ---------------- ENV ---------------- */

const {
  PORT = 3001,
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_ORDERS_TABLE = "Unfulfilled Orders Log",
  AIRTABLE_RETURNS_TABLE = "Incoming Returns",
  AIRTABLE_MERCHANTS_TABLE = "Merchants",
  AIRTABLE_RETURN_METHODS_TABLE = "Return Shipping Methods",
  SENDCLOUD_PUBLIC_KEY,
  SENDCLOUD_SECRET_KEY,
  SENDCLOUD_RETURNS_URL = "https://panel.sendcloud.sc/api/v3/returns",
  SENDCLOUD_FROM_NAME,
  SENDCLOUD_FROM_COMPANY,
  SENDCLOUD_FROM_ADDRESS_1,
  SENDCLOUD_FROM_CITY,
  SENDCLOUD_FROM_POSTAL_CODE,
  SENDCLOUD_FROM_COUNTRY = "NL",
  SENDCLOUD_FROM_EMAIL,
  SENDCLOUD_FROM_PHONE,
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET,
  R2_PUBLIC_BASE_URL,
  APP_PUBLIC_BASE_URL
} = process.env;

const required = [
  "AIRTABLE_TOKEN",
  "AIRTABLE_BASE_ID",
  "SENDCLOUD_PUBLIC_KEY",
  "SENDCLOUD_SECRET_KEY",
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET",
  "R2_PUBLIC_BASE_URL",
  "APP_PUBLIC_BASE_URL"
];

for (const key of required) {
  if (!process.env[key]) {
    throw new Error(`Missing required env var: ${key}`);
  }
}

/* ---------------- CLIENTS ---------------- */

const airtable = new Airtable({ apiKey: AIRTABLE_TOKEN }).base(AIRTABLE_BASE_ID);

const r2 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID,
    secretAccessKey: R2_SECRET_ACCESS_KEY
  }
});

/* ---------------- HELPERS ---------------- */

function first(value) {
  if (Array.isArray(value)) return value[0] ?? null;
  return value ?? null;
}

function asText(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function must(value, label) {
  const v = asText(value);
  if (!v) throw new Error(`Missing required value: ${label}`);
  return v;
}

function todayISO() {
  return new Date().toISOString();
}

function sanitizeFileName(name) {
  return name.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function buildBasicAuthHeader(publicKey, secretKey) {
  const token = Buffer.from(`${publicKey}:${secretKey}`).toString("base64");
  return `Basic ${token}`;
}

function getAttachmentUrl(fieldValue) {
  if (!Array.isArray(fieldValue) || !fieldValue[0]?.url) return "";
  return fieldValue[0].url;
}

async function fetchBuffer(url, headers = {}) {
  const res = await fetch(url, { headers });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Failed to fetch buffer from ${url}: ${res.status} ${text}`);
  }
  return Buffer.from(await res.arrayBuffer());
}

/* ---------------- AIRTABLE ---------------- */

async function getOrderRecord(orderRecordId) {
  return airtable(AIRTABLE_ORDERS_TABLE).find(orderRecordId);
}

async function findExistingReturnByLinkedOrder(orderRecordId) {
  const formula = `RECORD_ID() = '${orderRecordId}'`;

  const linkedOrderRecord = await airtable(AIRTABLE_ORDERS_TABLE)
    .select({
      filterByFormula: formula,
      maxRecords: 1
    })
    .firstPage();

  if (!linkedOrderRecord.length) {
    throw new Error(`Order record not found: ${orderRecordId}`);
  }

  const matching = await airtable(AIRTABLE_RETURNS_TABLE)
    .select({
      filterByFormula: `ARRAYJOIN({Linked Order}) = '${orderRecordId}'`,
      maxRecords: 1
    })
    .firstPage();

  return matching[0] || null;
}

async function createIncomingReturn(orderRecordId) {
  const created = await airtable(AIRTABLE_RETURNS_TABLE).create([
    {
      fields: {
        "Linked Order": [orderRecordId],
        "Return Status": "Registered"
      }
    }
  ]);
  return created[0];
}

async function getReturnRecord(returnRecordId) {
  return airtable(AIRTABLE_RETURNS_TABLE).find(returnRecordId);
}

async function getMerchantByClientId(clientId) {
  const records = await airtable(AIRTABLE_MERCHANTS_TABLE)
    .select({
      filterByFormula: `{Client ID} = '${clientId}'`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

async function updateReturnRecord(returnRecordId, fields) {
  return airtable(AIRTABLE_RETURNS_TABLE).update([
    {
      id: returnRecordId,
      fields
    }
  ]);
}

async function getReturnShippingOption(countryCode) {

  const records = await airtable(AIRTABLE_RETURN_METHODS_TABLE)
    .select({
      filterByFormula: `{Country Code} = '${countryCode}'`,
      maxRecords: 1
    })
    .firstPage();

  if (!records.length) {
    throw new Error(`No return shipping method configured for country ${countryCode}`);
  }

  const optionCode = asText(records[0].fields["Sendcloud Option Code"]);

  if (!optionCode) {
    throw new Error(`Sendcloud Option Code missing for country ${countryCode}`);
  }

  return optionCode;
}

/* ---------------- SENDCLOUD ---------------- */

function mapOrderToSendcloudPayload({ orderFields, returnId, shippingOptionCode }) {

  const customerName = asText(orderFields["Customer Name"]) || process.env.TEST_CUSTOMER_NAME;
  const companyName = asText(orderFields["Customer Company"]);

  const address1 = asText(orderFields["Shipping Address Line 1"]) || process.env.TEST_CUSTOMER_ADDRESS_1;
  const city = asText(orderFields["Shipping City"]) || process.env.TEST_CUSTOMER_CITY;
  const postalCode = asText(orderFields["Shipping Postal Code"]) || process.env.TEST_CUSTOMER_POSTAL_CODE;
  const country = asText(orderFields["Shipping Country Code"]) || process.env.TEST_CUSTOMER_COUNTRY;

  const email = asText(orderFields["Customer Email"]) || process.env.TEST_CUSTOMER_EMAIL;
  const phone = asText(orderFields["Customer Phone"]) || process.env.TEST_CUSTOMER_PHONE;

  return {

    from_address: {
      name: customerName || companyName || "Customer",
      address_line_1: address1,
      postal_code: postalCode,
      city,
      country_code: country,
      email: email || undefined,
      phone_number: phone || undefined
    },

    to_address: {
      name: SENDCLOUD_FROM_NAME,
      company_name: SENDCLOUD_FROM_COMPANY,
      address_line_1: SENDCLOUD_FROM_ADDRESS_1,
      postal_code: SENDCLOUD_FROM_POSTAL_CODE,
      city: SENDCLOUD_FROM_CITY,
      country_code: SENDCLOUD_FROM_COUNTRY,
      email: SENDCLOUD_FROM_EMAIL,
      phone_number: SENDCLOUD_FROM_PHONE
    },

    ship_with: {
      type: "shipping_option_code",
      shipping_option_code: shippingOptionCode
    },

    weight: {
      value: 0.5,
      unit: "kg"
    },

    external_reference: returnId
  };
}

async function createSendcloudReturnLabel({ orderFields, returnId }) {
  const countryCode = asText(orderFields["Shipping Country Code"]);
  const shippingOptionCode = await getReturnShippingOption(countryCode);
  const payload = mapOrderToSendcloudPayload({
    orderFields,
    returnId,
    shippingOptionCode
  });

  const res = await fetch(SENDCLOUD_RETURNS_URL, {
    method: "POST",
    headers: {
      Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY),
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const body = await res.json().catch(() => ({}));

  if (!res.ok) {
    throw new Error(`Sendcloud create return failed: ${res.status} ${JSON.stringify(body)}`);
  }

  const parcelId = body.parcel_id;

  if (!parcelId) {
    throw new Error(`Sendcloud return response missing parcel_id: ${JSON.stringify(body)}`);
  }

  // Wait briefly for label generation
  await new Promise(r => setTimeout(r, 1500));

  const labelRes = await fetch(
    `https://panel.sendcloud.sc/api/v3/parcel-documents?parcel_id=${parcelId}&type=label`,
    {
      headers: {
        Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY)
      }
    }
  );

  const labelData = await labelRes.json();

  const labelUrl = labelData?.documents?.[0]?.url;

  if (!labelUrl) {
    throw new Error(`Sendcloud label URL not found`);
  }

  return {
    parcelId: String(parcelId),
    trackingNumber: "",
    labelUrl
  };
}

/* ---------------- PDF GENERATION ---------------- */

function buildPackingSlipHtml({
  merchantName,
  merchantLogoUrl,
  brandColor,
  returnId,
  shopifyOrderNumber,
  productName,
  sku,
  size,
  qrCodeDataUrl
}) {
  const color = brandColor || "#111111";

  return `
  <!doctype html>
  <html>
    <head>
      <meta charset="utf-8" />
      <style>
        body {
          font-family: Arial, Helvetica, sans-serif;
          padding: 32px;
          color: #111;
          font-size: 14px;
        }
        .header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          border-bottom: 3px solid ${color};
          padding-bottom: 16px;
          margin-bottom: 24px;
        }
        .logo {
          max-height: 56px;
          max-width: 220px;
          object-fit: contain;
        }
        .title {
          font-size: 24px;
          font-weight: 700;
          color: ${color};
        }
        .box {
          border: 1px solid #ddd;
          border-radius: 8px;
          padding: 18px;
          margin-bottom: 20px;
        }
        .label {
          color: #666;
          font-size: 12px;
          margin-bottom: 4px;
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }
        .value {
          font-size: 16px;
          font-weight: 600;
          margin-bottom: 14px;
        }
        .grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 16px;
        }
        .qr {
          text-align: center;
          margin-top: 20px;
        }
        .qr img {
          width: 180px;
          height: 180px;
        }
        .footer {
          margin-top: 24px;
          font-size: 12px;
          color: #666;
          text-align: center;
        }
      </style>
    </head>
    <body>
      <div class="header">
        <div>
          ${merchantLogoUrl ? `<img class="logo" src="${merchantLogoUrl}" />` : `<div class="title">${merchantName}</div>`}
        </div>
        <div class="title">Return Packing Slip</div>
      </div>

      <div class="box">
        <div class="grid">
          <div>
            <div class="label">Return ID</div>
            <div class="value">${returnId}</div>
          </div>
          <div>
            <div class="label">Shopify Order</div>
            <div class="value">#${shopifyOrderNumber}</div>
          </div>
          <div>
            <div class="label">Product</div>
            <div class="value">${productName}</div>
          </div>
          <div>
            <div class="label">SKU</div>
            <div class="value">${sku}</div>
          </div>
          <div>
            <div class="label">Size</div>
            <div class="value">${size}</div>
          </div>
        </div>
      </div>

      <div class="qr">
        <img src="${qrCodeDataUrl}" />
        <div style="margin-top: 8px; font-weight: 700;">${returnId}</div>
      </div>

      <div class="footer">
        Please place this packing slip inside the parcel.<br />
        Processed via Lojiq Returns.
      </div>
    </body>
  </html>`;
}

async function htmlToPdfBuffer(html) {
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"]
  });

  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });
    const pdf = await page.pdf({
      format: "A4",
      printBackground: true
    });
    return Buffer.from(pdf);
  } finally {
    await browser.close();
  }
}

async function createPackingSlipPdf({ returnRecord, orderRecord, merchantRecord }) {
  const returnFields = returnRecord.fields;
  const orderFields = orderRecord.fields;
  const merchantFields = merchantRecord?.fields || {};

  const returnId = must(returnFields["Return ID"], "Return ID");
  const merchantName = asText(returnFields["Store Name"]) || asText(merchantFields["Store Name"]) || "Store";
  const merchantLogoUrl = getAttachmentUrl(merchantFields["Logo URL"]) || asText(merchantFields["Logo URL"]);
  const brandColor = asText(merchantFields["Brand Color"]);
  const shopifyOrderNumber = asText(returnFields["Shopify Order Number"]);
  const productName = asText(returnFields["Product Name"]);
  const sku = asText(returnFields["SKU"]);
  const size = asText(returnFields["Size"]);
  const scanUrl = `${APP_PUBLIC_BASE_URL.replace(/\/$/, "")}/scan/${encodeURIComponent(returnId)}`;

  const qrCodeDataUrl = await QRCode.toDataURL(scanUrl, {
    margin: 1,
    width: 360
  });

  const html = buildPackingSlipHtml({
    merchantName,
    merchantLogoUrl,
    brandColor,
    returnId,
    shopifyOrderNumber,
    productName,
    sku,
    size,
    qrCodeDataUrl
  });

  return htmlToPdfBuffer(html);
}

async function mergePdfBuffers(buffers) {
  const merged = await PDFDocument.create();

  for (const buffer of buffers) {
    const pdf = await PDFDocument.load(buffer);
    const copiedPages = await merged.copyPages(pdf, pdf.getPageIndices());
    copiedPages.forEach((page) => merged.addPage(page));
  }

  return Buffer.from(await merged.save());
}

/* ---------------- R2 ---------------- */

async function uploadReturnPackage({ returnId, pdfBuffer }) {
  const safe = sanitizeFileName(`${returnId}.pdf`);
  const key = `returns/${safe}`;

  await r2.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: key,
      Body: pdfBuffer,
      ContentType: "application/pdf"
    })
  );

  return `${R2_PUBLIC_BASE_URL.replace(/\/$/, "")}/${key}`;
}

/* ---------------- ROUTES ---------------- */

app.get("/", (_req, res) => {
  res.status(200).json({ ok: true, service: "lojiq-return-service" });
});

app.post("/create-return", async (req, res) => {
  try {
    const orderRecordId = asText(req.body?.order_record_id);
    if (!orderRecordId) {
      return res.status(400).json({ error: "Missing order_record_id" });
    }

    const existing = await findExistingReturnByLinkedOrder(orderRecordId);
    if (existing) {
      return res.status(200).json({
        already_exists: true,
        return_package_url: asText(existing.fields["Return Package URL"]),
        return_id: asText(existing.fields["Return ID"])
      });
    }

    const orderRecord = await getOrderRecord(orderRecordId);
    const createdReturn = await createIncomingReturn(orderRecordId);

    // Re-fetch so Airtable formula fields like Return ID are available.
    const returnRecord = await getReturnRecord(createdReturn.id);
    const returnFields = returnRecord.fields;
    const returnId = must(returnFields["Return ID"], "Return ID");

    const clientId = asText(first(orderRecord.fields["Client ID"])) || asText(orderRecord.fields["Client ID"]);
    const merchantRecord = clientId ? await getMerchantByClientId(clientId) : null;

    const sendcloud = await createSendcloudReturnLabel({
      orderFields: orderRecord.fields,
      returnId
    });

    const labelPdfBuffer = await fetchBuffer(sendcloud.labelUrl);
    const packingSlipBuffer = await createPackingSlipPdf({
      returnRecord,
      orderRecord,
      merchantRecord
    });

    const mergedBuffer = await mergePdfBuffers([labelPdfBuffer, packingSlipBuffer]);
    const returnPackageUrl = await uploadReturnPackage({
      returnId,
      pdfBuffer: mergedBuffer
    });

    await updateReturnRecord(returnRecord.id, {
      "Tracking Number": sendcloud.trackingNumber,
      "Sendcloud Parcel ID": sendcloud.parcelId,
      "Return Label URL": sendcloud.labelUrl,
      "Return Package URL": returnPackageUrl,
      "Return Status": "Label Generated"
    });

    return res.status(200).json({
      already_exists: false,
      return_id: returnId,
      return_package_url: returnPackageUrl
    });
  } catch (error) {
    console.error("/create-return failed:", error);
    return res.status(500).json({
      error: "Failed to create return",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Lojiq return service running on port ${PORT}`);
});
