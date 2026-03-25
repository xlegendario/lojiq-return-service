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

function drawLabelValue(page, label, value, x, y, font, bold, labelColor, valueColor) {
  page.drawText(label, {
    x,
    y,
    size: 9,
    font,
    color: labelColor
  });

  page.drawText(String(value || "-"), {
    x,
    y: y - 16,
    size: 13,
    font: bold,
    color: valueColor
  });
}

async function createPackingSlipPdf({
  merchantName,
  merchantLogoUrl,
  brandColor,
  returnId,
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

  const scanUrl = `${APP_PUBLIC_BASE_URL.replace(/\/$/, "")}/scan/${encodeURIComponent(returnId)}`;
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

  page.drawText("Return Packing Slip", {
    x: width - 210,
    y,
    size: 22,
    font: bold,
    color: primaryColor
  });

  y -= 70;

  // Return details box
  page.drawRectangle({
    x: margin,
    y: y - 140,
    width: width - margin * 2,
    height: 140,
    borderColor: lightGray,
    borderWidth: 1
  });

  const leftX = margin + 20;
  const rightX = margin + 280;
  let boxY = y - 25;

  drawLabelValue(page, "Return ID", returnId, leftX, boxY, font, bold, midGray, darkText);
  drawLabelValue(page, "Shopify Order", `#${orderNumber || "-"}`, rightX, boxY, font, bold, midGray, darkText);

  boxY -= 42;
  drawLabelValue(page, "Product", productName || "-", leftX, boxY, font, bold, midGray, darkText);
  drawLabelValue(page, "SKU", sku || "-", rightX, boxY, font, bold, midGray, darkText);

  boxY -= 42;
  drawLabelValue(page, "Size", size || "-", leftX, boxY, font, bold, midGray, darkText);

  y -= 175;

  // Instructions box
  page.drawRectangle({
    x: margin,
    y: y - 110,
    width: 320,
    height: 110,
    borderColor: lightGray,
    borderWidth: 1
  });

  page.drawText("Instructions", {
    x: margin + 20,
    y: y - 25,
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

  let instructionY = y - 48;
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

  page.drawText("Scan Return ID", {
    x: width - 150,
    y: y - 150,
    size: 10,
    font: bold,
    color: midGray
  });

  page.drawText(returnId, {
    x: width - 155,
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

async function createIncomingReturn(orderRecordId, orderRecord) {
  const orderFields = orderRecord.fields || {};

  const clientLinked = Array.isArray(orderFields["Client"])
    ? orderFields["Client"].filter(Boolean)
    : [];

  const shopifySellingPrice = orderFields["Selling Price"];
  const suggestedResalePrice = orderFields["Maximum Buying Price"];
  const matchRiskLevel = asText(orderFields["Match Risk Level"]);

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

/* ---------------- SENDCLOUD ---------------- */

function mapOrderToSendcloudPayload({ customerAddress, returnId, shippingOptionCode }) {

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

    external_reference: returnId
  };
}

async function createSendcloudReturnLabel({ customerAddress, returnId }) {

  const countryCode = customerAddress.country;
  const shippingOptionCode = await getReturnShippingOptionCode(countryCode);

  const payload = mapOrderToSendcloudPayload({
    customerAddress,
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
    const createdReturn = await createIncomingReturn(orderRecordId, orderRecord);

    // Re-fetch so Airtable formula fields like Return ID are available.
    const returnRecord = await getReturnRecord(createdReturn.id);
    const returnFields = returnRecord.fields;
    const returnId = must(returnFields["Return ID"], "Return ID");

    const sendcloud = await createSendcloudReturnLabel({
      customerAddress,
      returnId
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

app.listen(PORT, () => {
  console.log(`Lojiq return service running on port ${PORT}`);
});
