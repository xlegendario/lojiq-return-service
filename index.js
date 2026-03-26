import dotenv from "dotenv";
import express from "express";
import Airtable from "airtable";
import QRCode from "qrcode";
import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
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
  AIRTABLE_RETURNS_TABLE_ID,
  AIRTABLE_MERCHANTS_TABLE = "Merchants",
  AIRTABLE_RETURN_METHODS_TABLE = "Return Shipping Methods",
  SENDCLOUD_PUBLIC_KEY,
  SENDCLOUD_SECRET_KEY,
  SENDCLOUD_RETURNS_URL = "https://panel.sendcloud.sc/api/v3/returns",
  SENDCLOUD_TO_NAME,
  SENDCLOUD_TO_COMPANY,
  SENDCLOUD_TO_ADDRESS_1,
  SENDCLOUD_TO_CITY,
  SENDCLOUD_TO_POSTAL_CODE,
  SENDCLOUD_TO_COUNTRY = "NL",
  SENDCLOUD_TO_EMAIL,
  SENDCLOUD_TO_PHONE,
  R2_ACCOUNT_ID,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET,
  R2_PUBLIC_BASE_URL,
  APP_PUBLIC_BASE_URL,
  MAKE_MANUAL_RETURN_WEBHOOK_URL
} = process.env;

const required = [
  "AIRTABLE_TOKEN",
  "AIRTABLE_BASE_ID",
  "AIRTABLE_RETURNS_TABLE_ID",
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

function escapeAirtableFormulaValue(value) {
  return asText(value).replace(/'/g, "\\'");
}

function normalizeIncomingReturnVatType(orderVatType, overrideVatType) {
  const override = asText(overrideVatType).toLowerCase();
  if (override === "vat") return "VAT";
  if (override === "margin") return "Margin";

  const source = asText(orderVatType).toLowerCase();

  if (source === "vat21" || source === "vat0" || source === "vat") {
    return "VAT";
  }

  if (source === "margin") {
    return "Margin";
  }

  return "";
}

function todayISO() {
  return new Date().toISOString();
}

function sanitizeFileName(name) {
  return name.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function buildSendcloudOrderNumber(storeName, shopifyOrderNumber) {
  const cleanStore = asText(storeName)
    .replace(/\s+/g, "-")
    .replace(/[^a-zA-Z0-9-_]/g, "");

  const cleanShopifyOrderNumber = asText(shopifyOrderNumber)
    .replace(/\s+/g, "")
    .replace(/[^a-zA-Z0-9-_]/g, "");

  return [cleanStore, cleanShopifyOrderNumber].filter(Boolean).join("-");
}

function buildBasicAuthHeader(publicKey, secretKey) {
  const token = Buffer.from(`${publicKey}:${secretKey}`).toString("base64");
  return `Basic ${token}`;
}

function getAttachmentUrl(fieldValue) {
  if (!Array.isArray(fieldValue) || !fieldValue[0]?.url) return "";
  return fieldValue[0].url;
}

function getBrandColor(value) {
  const v = asText(value).toLowerCase();

  if (!v || v === "null" || v === "undefined") {
    return "#111111";
  }

  return v.startsWith("#") ? v : `#${v}`;
}

async function fetchImageBytes(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Failed to fetch image from ${url}: ${res.status} ${text}`);
  }
  return Buffer.from(await res.arrayBuffer());
}

async function fetchBuffer(url, headers = {}) {
  const res = await fetch(url, { headers });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Failed to fetch buffer from ${url}: ${res.status} ${text}`);
  }
  return Buffer.from(await res.arrayBuffer());
}

async function getShopifyOrder({ shopDomain, accessToken, orderId }) {

  const url = `https://${shopDomain}/admin/api/2024-01/orders/${orderId}.json`;

  const res = await fetch(url, {
    headers: {
      "X-Shopify-Access-Token": accessToken,
      "Content-Type": "application/json"
    }
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Shopify order fetch failed: ${res.status} ${text}`);
  }

  const data = await res.json();

  return data.order;
}

function extractCustomerAddress(shopifyOrder) {

  const addr = shopifyOrder.shipping_address;

  if (!addr) {
    throw new Error("Shopify order missing shipping address");
  }

  return {
    name: `${addr.first_name || ""} ${addr.last_name || ""}`.trim(),
    company: addr.company || "",
    address1: addr.address1,
    city: addr.city,
    postalCode: addr.zip,
    country: addr.country_code,
    email: shopifyOrder.email,
    phone: addr.phone
  };
}

async function findShopifyOrderByOrderNumber({ shopDomain, accessToken, shopifyOrderNumber }) {
  const normalizedOrderNumber = asText(shopifyOrderNumber).replace(/^#/, "");
  const orderName = `#${normalizedOrderNumber}`;

  const url = `https://${shopDomain}/admin/api/2024-10/orders.json?status=any&limit=1&name=${encodeURIComponent(orderName)}`;

  const res = await fetch(url, {
    headers: {
      "X-Shopify-Access-Token": accessToken,
      "Content-Type": "application/json"
    }
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Shopify order lookup failed: ${res.status} ${text}`);
  }

  const data = await res.json();
  const order = data?.orders?.[0];

  if (!order) {
    throw new Error(`Shopify order not found for order number ${normalizedOrderNumber}`);
  }

  return order;
}

function extractReturnableItemsFromShopifyOrder(shopifyOrder) {
  const lineItems = Array.isArray(shopifyOrder?.line_items) ? shopifyOrder.line_items : [];

  return lineItems.map((item) => ({
    line_item_id: String(item.id),
    product_name: asText(item.title),
    sku: asText(item.sku),
    size: asText(item.variant_title),
    quantity: item.quantity ?? 1,
    selling_price: asText(item.price)
  }));
}

function rgbFromHex(hex) {
  const clean = (hex || "#111111").replace("#", "");
  const normalized = clean.length === 3
    ? clean.split("").map((c) => c + c).join("")
    : clean;

  const num = parseInt(normalized, 16);
  const r = ((num >> 16) & 255) / 255;
  const g = ((num >> 8) & 255) / 255;
  const b = (num & 255) / 255;

  return rgb(r, g, b); // ✅ THIS is the fix
}

function drawWrappedText(page, text, x, y, maxWidth, font, size, color, lineHeight = 15, maxLines = 3) {
  const words = String(text || "-").split(/\s+/);
  const lines = [];
  let currentLine = "";

  for (const word of words) {
    const testLine = currentLine ? `${currentLine} ${word}` : word;
    const testWidth = font.widthOfTextAtSize(testLine, size);

    if (testWidth <= maxWidth) {
      currentLine = testLine;
    } else {
      if (currentLine) lines.push(currentLine);
      currentLine = word;
    }
  }

  if (currentLine) lines.push(currentLine);

  const finalLines = lines.slice(0, maxLines);

  finalLines.forEach((line, index) => {
    page.drawText(line, {
      x,
      y: y - index * lineHeight,
      size,
      font,
      color
    });
  });

  return finalLines.length;
}

function drawLabelValue(page, label, value, x, y, font, bold, labelColor, valueColor, options = {}) {
  const {
    maxWidth = 200,
    maxLines = 1,
    valueSize = 13,
    lineHeight = 15
  } = options;

  page.drawText(label, {
    x,
    y,
    size: 9,
    font,
    color: labelColor
  });

  const linesUsed = drawWrappedText(
    page,
    String(value || "-"),
    x,
    y - 16,
    maxWidth,
    bold,
    valueSize,
    valueColor,
    lineHeight,
    maxLines
  );

  return linesUsed;
}

function drawCheckboxList(page, items, x, y, font, textColor, lineHeight = 18) {
  items.forEach((item, index) => {
    const yy = y - index * lineHeight;

    page.drawRectangle({
      x,
      y: yy - 8,
      width: 10,
      height: 10,
      borderWidth: 1,
      borderColor: textColor
    });

    page.drawText(item, {
      x: x + 18,
      y: yy - 7,
      size: 10.5,
      font,
      color: textColor
    });
  });
}

async function createPackingSlipPdf({
  merchantName,
  merchantLogoUrl,
  brandColor,
  returnId,
  airtableRecordId,
  orderNumber,
  productName,
  sku,
  size
}) {
  const pdf = await PDFDocument.create();
  const page = pdf.addPage([595, 842]);

  const { width, height } = page.getSize();

  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);

  const primaryColor = rgbFromHex(brandColor || "#111111");
  const lightGray = rgbFromHex("#E5E7EB");
  const midGray = rgbFromHex("#6B7280");
  const darkText = rgbFromHex("#111827");

  const scanUrl = `https://airtable.com/${AIRTABLE_BASE_ID}/${AIRTABLE_RETURNS_TABLE_ID}/${airtableRecordId}`;
  const qrDataUrl = await QRCode.toDataURL(scanUrl, { margin: 1, width: 300 });

  const qrImageBytes = Buffer.from(
    qrDataUrl.replace(/^data:image\/png;base64,/, ""),
    "base64"
  );
  const qrImage = await pdf.embedPng(qrImageBytes);

  let logoImage = null;
  if (merchantLogoUrl) {
    try {
      const logoBytes = await fetchImageBytes(merchantLogoUrl);

      if (merchantLogoUrl.toLowerCase().includes(".png")) {
        logoImage = await pdf.embedPng(logoBytes);
      } else {
        try {
          logoImage = await pdf.embedJpg(logoBytes);
        } catch {
          logoImage = await pdf.embedPng(logoBytes);
        }
      }
    } catch (err) {
      console.error("Could not load merchant logo:", err.message);
    }
  }

  const margin = 40;
  let y = height - 50;

  // Header line
  page.drawRectangle({
    x: margin,
    y: y - 8,
    width: width - margin * 2,
    height: 3,
    color: primaryColor
  });

  y -= 35;

  // Logo or merchant name
  if (logoImage) {
    const maxLogoWidth = 160;
    const maxLogoHeight = 50;
    const scale = Math.min(
      maxLogoWidth / logoImage.width,
      maxLogoHeight / logoImage.height
    );
    const logoWidth = logoImage.width * scale;
    const logoHeight = logoImage.height * scale;

    page.drawImage(logoImage, {
      x: margin,
      y: y - logoHeight + 10,
      width: logoWidth,
      height: logoHeight
    });
  } else {
    page.drawText(merchantName || "Store", {
      x: margin,
      y,
      size: 20,
      font: bold,
      color: darkText
    });
  }

  const title = "Return Packing Slip";
  const titleSize = 22;
  const titleWidth = bold.widthOfTextAtSize(title, titleSize);
  
  page.drawText(title, {
    x: width - margin - titleWidth,
    y,
    size: titleSize,
    font: bold,
    color: primaryColor
  });

  y -= 70;

  // Return details box
  page.drawRectangle({
    x: margin,
    y: y - 165,
    width: width - margin * 2,
    height: 165,
    borderColor: lightGray,
    borderWidth: 1
  });

  const leftX = margin + 20;
  const rightX = margin + 280;
  let boxY = y - 25;

  drawLabelValue(page, "Return ID", returnId, leftX, boxY, font, bold, midGray, darkText);
  drawLabelValue(page, "Shopify Order", `#${orderNumber || "-"}`, rightX, boxY, font, bold, midGray, darkText);

  boxY -= 42;
  const productLines = drawLabelValue(
    page,
    "Product",
    productName || "-",
    leftX,
    boxY,
    font,
    bold,
    midGray,
    darkText,
    {
      maxWidth: 250,
      maxLines: 3,
      valueSize: 11.5,
      lineHeight: 14
    }
  );
  
  drawLabelValue(
    page,
    "SKU",
    sku || "-",
    rightX,
    boxY,
    font,
    bold,
    midGray,
    darkText,
    {
      maxWidth: 120,
      maxLines: 2
    }
  );

  boxY -= 42 + ((productLines - 1) * 14);

  drawLabelValue(
    page,
    "Size",
    size || "-",
    leftX,
    boxY,
    font,
    bold,
    midGray,
    darkText
  );
  
  y -= 200;

  const returnReasons = [
    "Too small",
    "Too big",
    "Not as expected",
    "Changed mind",
    "Damaged",
    "Wrong item received",
    "Defective",
    "Other"
  ];

  // Instructions box
  page.drawRectangle({
    x: margin,
    y: y - 285,
    width: 320,
    height: 310,
    borderColor: lightGray,
    borderWidth: 1
  });

  page.drawText("Return Reason", {
    x: margin + 20,
    y: y - 25,
    size: 14,
    font: bold,
    color: primaryColor
  });
  
  drawCheckboxList(
    page,
    returnReasons,
    margin + 20,
    y - 48,
    font,
    darkText,
    18
  );
  
  page.drawText("Instructions", {
    x: margin + 20,
    y: y - 205,
    size: 14,
    font: bold,
    color: primaryColor
  });
  
  const instructionLines = [
    "1. Print this packing slip.",
    "2. Place this slip inside the parcel.",
    "3. Attach the return label to the outside of the box.",
    "4. Drop off the parcel at the carrier point."
  ];
  
  let instructionY = y - 228;
  for (const line of instructionLines) {
    page.drawText(line, {
      x: margin + 20,
      y: instructionY,
      size: 10.5,
      font,
      color: darkText
    });
    instructionY -= 16;
  }

  // QR block
  page.drawRectangle({
    x: width - 180,
    y: y - 180,
    width: 140,
    height: 180,
    borderColor: lightGray,
    borderWidth: 1
  });

  page.drawImage(qrImage, {
    x: width - 160,
    y: y - 135,
    width: 100,
    height: 100
  });

  const qrCenterX = width - 110;

  page.drawText("Scan Return ID", {
    x: qrCenterX - (bold.widthOfTextAtSize("Scan Return ID", 10) / 2),
    y: y - 150,
    size: 10,
    font: bold,
    color: midGray
  });
  
  page.drawText(returnId, {
    x: qrCenterX - (bold.widthOfTextAtSize(returnId, 11) / 2),
    y: y - 165,
    size: 11,
    font: bold,
    color: darkText
  });

  // Footer
  page.drawText("Generated by Lojiq Returns", {
    x: margin,
    y: 30,
    size: 9,
    font,
    color: midGray
  });

  return Buffer.from(await pdf.save());
}

async function mergePdfBuffers(buffers) {
  const merged = await PDFDocument.create();

  for (const buffer of buffers) {
    const pdf = await PDFDocument.load(buffer);
    const pages = await merged.copyPages(pdf, pdf.getPageIndices());
    pages.forEach((page) => merged.addPage(page));
  }

  return Buffer.from(await merged.save());
}

async function uploadReturnPackage({ returnId, pdfBuffer }) {

  const key = `returns/${sanitizeFileName(returnId)}.pdf`;

  await r2.send(
    new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: key,
      Body: pdfBuffer,
      ContentType: "application/pdf"
    })
  );

  return `${R2_PUBLIC_BASE_URL}/${key}`;
}

async function triggerMakeManualReturnEnrichment({
  returnRecordId,
  merchantRecord,
  shopifyOrderNumber,
  shopifyOrderId,
  lineItemId,
  productName,
  sku,
  size,
  sellingPrice,
  vatType
}) {
  if (!MAKE_MANUAL_RETURN_WEBHOOK_URL) {
    console.warn("MAKE_MANUAL_RETURN_WEBHOOK_URL not configured, skipping Make trigger");
    return;
  }

  const merchantFields = merchantRecord.fields || {};

  const res = await fetch(MAKE_MANUAL_RETURN_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      return_record_id: returnRecordId,
      merchant_record_id: merchantRecord.id,
      store_name: asText(merchantFields["Store Name"]),
      shopify_order_number: asText(shopifyOrderNumber),
      shopify_order_id: asText(shopifyOrderId),
      line_item_id: asText(lineItemId),
      product_name: asText(productName),
      sku: asText(sku),
      size: asText(size),
      selling_price: asText(sellingPrice)
      vat_type: asText(vatType)
    })
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Make webhook failed: ${res.status} ${text}`);
  }
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

async function createIncomingReturn(orderRecordId, orderRecord, vatTypeOverride = "") {
  const orderFields = orderRecord.fields || {};

  const clientLinked = Array.isArray(orderFields["Client"])
    ? orderFields["Client"].filter(Boolean)
    : [];

  const shopifySellingPrice = orderFields["Selling Price"];
  const suggestedResalePrice = orderFields["Maximum Buying Price"];
  const matchRiskLevel = asText(orderFields["Match Risk Level"]);
  const incomingReturnVatType = normalizeIncomingReturnVatType(
    orderFields["VAT Type"],
    vatTypeOverride
  );

  const created = await airtable(AIRTABLE_RETURNS_TABLE).create([
    {
      fields: {
        "Linked Order": [orderRecordId],
        "Return Status": "Registered",

        "Store Name": asText(orderFields["Store Name"]),
        "Shopify Order Number": asText(orderFields["Shopify Order Number"]),
        "Product Name": asText(orderFields["Product Name"]),
        "SKU": asText(orderFields["SKU"]),
        "Size": asText(orderFields["Size"]),

        "Shopify Selling Price": shopifySellingPrice ?? null,
        "Suggested Resale Price": suggestedResalePrice ?? null,

        "Match Risk Level": matchRiskLevel || null,
        "VAT Type": incomingReturnVatType || null,

        "Client": clientLinked
      }
    }
  ]);

  return created[0];
}

async function getReturnRecord(returnRecordId) {
  return airtable(AIRTABLE_RETURNS_TABLE).find(returnRecordId);
}

async function getMerchantByClientId(clientId) {
  try {
    return await airtable(AIRTABLE_MERCHANTS_TABLE).find(clientId);
  } catch {
    return null;
  }
}

async function updateReturnRecord(returnRecordId, fields) {
  return airtable(AIRTABLE_RETURNS_TABLE).update([
    {
      id: returnRecordId,
      fields
    }
  ]);
}

async function getReturnShippingOptionCode(countryCode) {

  const records = await airtable(AIRTABLE_RETURN_METHODS_TABLE)
    .select({
      filterByFormula: `{Country Code} = '${countryCode}'`,
      maxRecords: 1
    })
    .firstPage();

  if (!records.length) {
    throw new Error(`No return shipping option configured for country ${countryCode}`);
  }

  const option = records[0].fields["Shipping Option Code"];

  if (!option) {
    throw new Error(`Shipping Option Code missing for country ${countryCode}`);
  }

  return option;
}

async function getMerchantBySubmitReturnChannelId(channelId) {
  const safeChannelId = escapeAirtableFormulaValue(channelId);

  const records = await airtable(AIRTABLE_MERCHANTS_TABLE)
    .select({
      filterByFormula: `{Submit Return Channel ID} = '${safeChannelId}'`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

async function findExistingReturnByClientOrderAndLineItem(clientId, shopifyOrderNumber, lineItemId) {
  const safeClientId = escapeAirtableFormulaValue(clientId);
  const safeOrderNumber = escapeAirtableFormulaValue(shopifyOrderNumber);
  const safeLineItemId = escapeAirtableFormulaValue(lineItemId);

  const records = await airtable(AIRTABLE_RETURNS_TABLE)
    .select({
      filterByFormula: `AND(
        ARRAYJOIN({Client}) = '${safeClientId}',
        {Shopify Order Number} = '${safeOrderNumber}',
        {Shopify Line Item ID} = ${Number(safeLineItemId)}
      )`,
      maxRecords: 1
    })
    .firstPage();

  return records[0] || null;
}

async function createManualIncomingReturn({
  merchantRecord,
  shopifyOrderNumber,
  shopifyOrderId,
  lineItemId,
  productName,
  sku,
  size,
  sellingPrice,
  vatType
}) {
  const merchantFields = merchantRecord.fields || {};
  const clientLinked = [merchantRecord.id];

  const created = await airtable(AIRTABLE_RETURNS_TABLE).create([
    {
      fields: {
        "Return Status": "Pending Enrichment",
        "Store Name": asText(merchantFields["Store Name"]),
        "Shopify Order Number": asText(shopifyOrderNumber),
        "Shopify Order ID": asText(shopifyOrderId),
        "Shopify Line Item ID": asText(lineItemId),
        "Product Name": asText(productName),
        "SKU": asText(sku),
        "Size": asText(size),
        "Shopify Selling Price": sellingPrice || null,
        "VAT Type": normalizeIncomingReturnVatType("", vatType) || null,
        "Client": clientLinked
      }
    }
  ]);

  return created[0];
}

/* ---------------- SENDCLOUD ---------------- */

function mapOrderToSendcloudPayload({
  customerAddress,
  returnId,
  shippingOptionCode,
  sendcloudOrderNumber
}) {

  const {
    name,
    company,
    address1,
    city,
    postalCode,
    country,
    email,
    phone
  } = customerAddress;

  return {
    from_address: {
      name,
      company_name: company || undefined,
      address_line_1: address1,
      postal_code: postalCode,
      city,
      country_code: country,
      email: email || undefined,
      phone_number: phone || undefined
    },

    to_address: {
      name: SENDCLOUD_TO_NAME,
      company_name: SENDCLOUD_TO_COMPANY || undefined,
      address_line_1: SENDCLOUD_TO_ADDRESS_1,
      postal_code: SENDCLOUD_TO_POSTAL_CODE,
      city: SENDCLOUD_TO_CITY,
      country_code: SENDCLOUD_TO_COUNTRY,
      email: SENDCLOUD_TO_EMAIL || undefined,
      phone_number: SENDCLOUD_TO_PHONE || undefined
    },

    ship_with: {
      type: "shipping_option_code",
      shipping_option_code: shippingOptionCode
    },

    weight: {
      value: 0.5,
      unit: "kg"
    },

    order_number: sendcloudOrderNumber || undefined,
    external_reference: returnId
  };
}

async function createSendcloudReturnLabel({
  customerAddress,
  returnId,
  storeName,
  shopifyOrderNumber
}) {

  const countryCode = customerAddress.country;
  const shippingOptionCode = await getReturnShippingOptionCode(countryCode);
  const sendcloudOrderNumber = buildSendcloudOrderNumber(storeName, shopifyOrderNumber);

  const payload = mapOrderToSendcloudPayload({
    customerAddress,
    returnId,
    shippingOptionCode,
    sendcloudOrderNumber
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
  const sendcloudReturnId = body.return_id;

  if (!sendcloudReturnId) {
    throw new Error(`Sendcloud return response missing return_id: ${JSON.stringify(body)}`);
  }

  let labelUrl = null;

  // Poll return details until label exists
  for (let i = 0; i < 20; i++) {

    await new Promise(r => setTimeout(r, 1500));

    const returnRes = await fetch(
      `https://panel.sendcloud.sc/api/v3/returns/${sendcloudReturnId}`,
      {
        headers: {
          Authorization: buildBasicAuthHeader(SENDCLOUD_PUBLIC_KEY, SENDCLOUD_SECRET_KEY)
        }
      }
    );

    const returnData = await returnRes.json();

    labelUrl = returnData?.label?.label_printer;

    if (labelUrl) {
      console.log("Sendcloud label ready:", labelUrl);
      break;
    }

    console.log("Waiting for Sendcloud label...");
  }

  if (!labelUrl) {
    throw new Error("Sendcloud label generation timeout");
  }

  return {
    parcelId: String(parcelId),
    trackingNumber: "",
    labelUrl
  };
}

/* ---------------- PDF GENERATION ---------------- */

/* ---------------- R2 ---------------- */



/* ---------------- ROUTES ---------------- */

app.get("/", (_req, res) => {
  res.status(200).json({ ok: true, service: "lojiq-return-service" });
});

app.post("/create-return", async (req, res) => {
  try {
    const orderRecordId = asText(req.body?.order_record_id);
    const vatTypeOverride = asText(req.body?.vat_type);
    if (!orderRecordId) {
      return res.status(400).json({ error: "Missing order_record_id" });
    }

    const existing = await findExistingReturnByLinkedOrder(orderRecordId);
    if (existing) {
      return res.status(200).json({
        already_exists: true,
        return_package_url: asText(existing.fields["Packing Slip URL"]),
        return_id: asText(existing.fields["Return ID"])
      });
    }

    const orderRecord = await getOrderRecord(orderRecordId);
    const clientRecord = first(orderRecord.fields["Client"]);

    if (!clientRecord) {
      throw new Error("Client not linked on order record");
    }
    
    const merchantRecord = await airtable(AIRTABLE_MERCHANTS_TABLE).find(clientRecord);
    const merchantFields = merchantRecord.fields;

    const shopDomain = asText(merchantFields["Shopify Store URL"])
      .replace(/^https?:\/\//, "")
      .replace(/\/$/, "");
    
    const accessToken = asText(merchantFields["Shopify Token"]);
    const shopifyOrderId = asText(orderRecord.fields["Shopify Order ID"]);

    if (!shopifyOrderId) {
      throw new Error("Shopify Order ID missing in Airtable order");
    }
    const shopifyOrder = await getShopifyOrder({
      shopDomain,
      accessToken,
      orderId: shopifyOrderId
    });
    const customerAddress = extractCustomerAddress(shopifyOrder);
    const createdReturn = await createIncomingReturn(
      orderRecordId,
      orderRecord,
      vatTypeOverride
    );

    // Re-fetch so Airtable formula fields like Return ID are available.
    const returnRecord = await getReturnRecord(createdReturn.id);
    const returnFields = returnRecord.fields;
    const returnId = must(returnFields["Return ID"], "Return ID");

    const sendcloud = await createSendcloudReturnLabel({
      customerAddress,
      returnId,
      storeName: asText(orderRecord.fields["Store Name"]),
      shopifyOrderNumber: asText(orderRecord.fields["Shopify Order Number"])
    });
    
    // download label
    const labelPdf = await fetchBuffer(
      sendcloud.labelUrl,
      {
        Authorization: buildBasicAuthHeader(
          SENDCLOUD_PUBLIC_KEY,
          SENDCLOUD_SECRET_KEY
        )
      }
    );
    
    // create packing slip
    const packingSlipPdf = await createPackingSlipPdf({
      merchantName: asText(merchantFields["Store Name"]) || asText(returnFields["Store Name"]) || "Store",
      merchantLogoUrl: getAttachmentUrl(merchantFields["Logo URL"]) || asText(merchantFields["Logo URL"]),
      brandColor: asText(merchantFields["Brand Color"]) || "#111111",
      returnId,
      airtableRecordId: returnRecord.id,
      orderNumber: asText(returnFields["Shopify Order Number"]),
      productName: asText(returnFields["Product Name"]),
      sku: asText(returnFields["SKU"]),
      size: asText(returnFields["Size"])
    });
    
    // merge PDFs
    const mergedPdf = await mergePdfBuffers([
      labelPdf,
      packingSlipPdf
    ]);
    
    // upload to R2
    const returnPackageUrl = await uploadReturnPackage({
      returnId,
      pdfBuffer: mergedPdf
    });

    await updateReturnRecord(returnRecord.id, {
      "Tracking Number": sendcloud.trackingNumber,
      "Sendcloud Parcel ID": sendcloud.parcelId,
      "Return Label URL": sendcloud.labelUrl,
      "Packing Slip URL": returnPackageUrl,
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

app.post("/create-manual-return", async (req, res) => {
  try {
    const submitChannelId = asText(req.body?.submit_channel_id);
    const shopifyOrderNumber = asText(req.body?.shopify_order_number);
    const shopifyOrderId = asText(req.body?.shopify_order_id);
    const lineItemId = asText(req.body?.line_item_id);
    const productName = asText(req.body?.product_name);
    const sku = asText(req.body?.sku);
    const size = asText(req.body?.size);
    const sellingPrice = asText(req.body?.selling_price);
    const vatType = asText(req.body?.vat_type);

    if (!submitChannelId) {
      return res.status(400).json({ error: "Missing submit_channel_id" });
    }

    if (!shopifyOrderNumber) {
      return res.status(400).json({ error: "Missing shopify_order_number" });
    }

    if (!/^\d+$/.test(shopifyOrderNumber)) {
      return res.status(400).json({
        error: "shopify_order_number must contain digits only"
      });
    }

    if (!["VAT", "Margin"].includes(vatType)) {
      return res.status(400).json({
        error: "vat_type must be VAT or Margin"
      });
    }

    if (!shopifyOrderId) {
      return res.status(400).json({ error: "Missing shopify_order_id" });
    }
    
    if (!lineItemId) {
      return res.status(400).json({ error: "Missing line_item_id" });
    }
    
    if (!productName) {
      return res.status(400).json({ error: "Missing product_name" });
    }

    const merchantRecord = await getMerchantBySubmitReturnChannelId(submitChannelId);

    if (!merchantRecord) {
      return res.status(404).json({
        error: "No merchant found for this submit channel"
      });
    }

    const merchantFields = merchantRecord.fields || {};
    const clientRecordId = merchantRecord.id;

    const existing = await findExistingReturnByClientOrderAndLineItem(
      clientRecordId,
      shopifyOrderNumber,
      lineItemId
    );

    if (existing) {
      return res.status(200).json({
        already_exists: true,
        return_record_id: existing.id,
        return_id: asText(existing.fields["Return ID"]),
        return_package_url: asText(existing.fields["Packing Slip URL"]),
        returns_channel_id: asText(merchantFields["Returns Channel ID"]),
        store_name: asText(merchantFields["Store Name"])
      });
    }

    const createdReturn = await createManualIncomingReturn({
      merchantRecord,
      shopifyOrderNumber,
      shopifyOrderId,
      lineItemId,
      productName,
      sku,
      size,
      sellingPrice,
      vatType
    });
    
    let makeTriggered = true;
    let makeError = "";
    
    try {
      await triggerMakeManualReturnEnrichment({
        returnRecordId: createdReturn.id,
        merchantRecord,
        shopifyOrderNumber,
        shopifyOrderId,
        lineItemId,
        productName,
        sku,
        size,
        sellingPrice,
        vatType
      });
    } catch (err) {
      makeTriggered = false;
      makeError = err.message;
      console.error("Failed to trigger Make enrichment:", err);
    }
    
    return res.status(200).json({
      ok: true,
      already_exists: false,
      return_record_id: createdReturn.id,
      returns_channel_id: asText(merchantFields["Returns Channel ID"]),
      store_name: asText(merchantFields["Store Name"]),
      make_triggered: makeTriggered,
      make_error: makeError || undefined
    });
    
  } catch (error) {
    console.error("/create-manual-return failed:", error);
    return res.status(500).json({
      error: "Failed to create manual return",
      details: error.message
    });
  }
});

app.post("/lookup-manual-return-order", async (req, res) => {
  try {
    const submitChannelId = asText(req.body?.submit_channel_id);
    const shopifyOrderNumber = asText(req.body?.shopify_order_number);

    if (!submitChannelId) {
      return res.status(400).json({ error: "Missing submit_channel_id" });
    }

    if (!shopifyOrderNumber) {
      return res.status(400).json({ error: "Missing shopify_order_number" });
    }

    const merchantRecord = await getMerchantBySubmitReturnChannelId(submitChannelId);

    if (!merchantRecord) {
      return res.status(404).json({
        error: "No merchant found for this submit channel"
      });
    }

    const merchantFields = merchantRecord.fields || {};

    const shopDomain = asText(merchantFields["Shopify Store URL"])
      .replace(/^https?:\/\//, "")
      .replace(/\/$/, "");

    const accessToken = asText(merchantFields["Shopify Token"]);

    if (!shopDomain || !accessToken) {
      throw new Error("Merchant Shopify credentials missing");
    }

    const shopifyOrder = await findShopifyOrderByOrderNumber({
      shopDomain,
      accessToken,
      shopifyOrderNumber
    });

    const items = extractReturnableItemsFromShopifyOrder(shopifyOrder);

    if (!items.length) {
      throw new Error("No line items found on Shopify order");
    }

    return res.status(200).json({
      ok: true,
      shopify_order_id: String(shopifyOrder.id),
      shopify_order_number: asText(shopifyOrder.name || shopifyOrderNumber).replace(/^#/, ""),
      items
    });
  } catch (error) {
    console.error("/lookup-manual-return-order failed:", error);
    return res.status(500).json({
      error: "Failed to lookup manual return order",
      details: error.message
    });
  }
});

app.post("/process-existing-return", async (req, res) => {
  try {
    const returnRecordId = asText(req.body?.return_record_id);

    if (!returnRecordId) {
      return res.status(400).json({ error: "Missing return_record_id" });
    }

    const returnRecord = await getReturnRecord(returnRecordId);
    const returnFields = returnRecord.fields || {};

    const existingPackageUrl = asText(returnFields["Packing Slip URL"]);
    const existingReturnId = asText(returnFields["Return ID"]);
    const currentStatus = asText(returnFields["Return Status"]);
    
    if (existingPackageUrl && currentStatus === "Label Generated") {
      return res.status(200).json({
        ok: true,
        already_exists: true,
        return_record_id: returnRecord.id,
        return_id: existingReturnId,
        return_package_url: existingPackageUrl,
        returns_channel_id:
          asText(returnFields["Returns Channel ID"]),
        store_name: asText(returnFields["Store Name"]),
        product_name: asText(returnFields["Product Name"]),
        sku: asText(returnFields["SKU"]),
        size: asText(returnFields["Size"]),
        vat_type: asText(returnFields["VAT Type"]),
        shopify_order_number: asText(returnFields["Shopify Order Number"])
      });
    }

    const clientRecord = first(returnFields["Client"]);
    if (!clientRecord) {
      throw new Error("Client not linked on return record");
    }

    const merchantRecord = await getMerchantByClientId(clientRecord);
    if (!merchantRecord) {
      throw new Error(`Merchant not found for client ${clientRecord}`);
    }

    const merchantFields = merchantRecord.fields;

    const shopDomain = asText(merchantFields["Shopify Store URL"])
      .replace(/^https?:\/\//, "")
      .replace(/\/$/, "");

    const accessToken = asText(merchantFields["Shopify Token"]);
    const shopifyOrderId = asText(returnFields["Shopify Order ID"]);

    if (!shopifyOrderId) {
      throw new Error("Shopify Order ID missing on Incoming Return");
    }

    await updateReturnRecord(returnRecord.id, {
      "Return Status": "Processing",
    });

    const shopifyOrder = await getShopifyOrder({
      shopDomain,
      accessToken,
      orderId: shopifyOrderId
    });

    const customerAddress = extractCustomerAddress(shopifyOrder);
    const returnId = must(returnFields["Return ID"], "Return ID");

    const sendcloud = await createSendcloudReturnLabel({
      customerAddress,
      returnId,
      storeName: asText(returnFields["Store Name"]),
      shopifyOrderNumber: asText(returnFields["Shopify Order Number"])
    });

    const labelPdf = await fetchBuffer(
      sendcloud.labelUrl,
      {
        Authorization: buildBasicAuthHeader(
          SENDCLOUD_PUBLIC_KEY,
          SENDCLOUD_SECRET_KEY
        )
      }
    );

    const packingSlipPdf = await createPackingSlipPdf({
      merchantName: asText(merchantFields["Store Name"]) || asText(returnFields["Store Name"]) || "Store",
      merchantLogoUrl: getAttachmentUrl(merchantFields["Logo URL"]) || asText(merchantFields["Logo URL"]),
      brandColor: asText(merchantFields["Brand Color"]) || "#111111",
      returnId,
      airtableRecordId: returnRecord.id,
      orderNumber: asText(returnFields["Shopify Order Number"]),
      productName: asText(returnFields["Product Name"]),
      sku: asText(returnFields["SKU"]),
      size: asText(returnFields["Size"])
    });

    const mergedPdf = await mergePdfBuffers([
      labelPdf,
      packingSlipPdf
    ]);

    const returnPackageUrl = await uploadReturnPackage({
      returnId,
      pdfBuffer: mergedPdf
    });

    await updateReturnRecord(returnRecord.id, {
      "Tracking Number": sendcloud.trackingNumber,
      "Sendcloud Parcel ID": sendcloud.parcelId,
      "Return Label URL": sendcloud.labelUrl,
      "Packing Slip URL": returnPackageUrl,
      "Return Status": "Label Generated",
    });

    return res.status(200).json({
      ok: true,
      return_record_id: returnRecord.id,
      return_id: returnId,
      return_package_url: returnPackageUrl,
      returns_channel_id:
        asText(returnFields["Returns Channel ID"]) ||
        asText(merchantFields["Returns Channel ID"]),
      store_name: asText(returnFields["Store Name"]),
      product_name: asText(returnFields["Product Name"]),
      sku: asText(returnFields["SKU"]),
      size: asText(returnFields["Size"]),
      vat_type: asText(returnFields["VAT Type"]),
      shopify_order_number: asText(returnFields["Shopify Order Number"])
    });
  } catch (error) {
    console.error("/process-existing-return failed:", error);

    const returnRecordId = asText(req.body?.return_record_id);
    if (returnRecordId) {
      try {
        await updateReturnRecord(returnRecordId, {
          "Return Status": "Failed",
        });
      } catch (updateError) {
        console.error("Failed to mark return as failed:", updateError);
      }
    }

    return res.status(500).json({
      error: "Failed to process existing return",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Lojiq return service running on port ${PORT}`);
});
