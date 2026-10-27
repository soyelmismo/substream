// substream-media-proxy v2.0 — Cloudflare Worker para Offload de Audio y Metadatos de Substream
// Intercepta /rest/stream, /rest/download en sound.listo.click
// Descarga y sirve el audio directamente desde los CDNs (Tidal, Monochrome, Qobuz, Deezer, etc.)
// hacia el cliente (Supersonic, Symfonium, etc.), aliviando el 100% del ancho de banda multimedia de lampara.
// En /rest/download, además inyecta los bloques de metadatos Vorbis Comment y Cover Art al vuelo.

const RETRY_ATTEMPTS = 3;

const FORWARD_RESPONSE_HEADERS = [
  "content-type",
  "content-length",
  "content-range",
  "accept-ranges",
  "etag",
  "last-modified",
  "cache-control",
  "content-disposition"
];

function copyHeaders(src, dst) {
  for (const h of FORWARD_RESPONSE_HEADERS) {
    const val = src.get(h);
    if (val !== null) {
      dst.set(h, val);
    }
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Endpoint de comprobación de salud del worker
    if (url.pathname === "/_worker/health") {
      return new Response("substream-media-proxy OK", { status: 200 });
    }

    const isStreamPath = url.pathname.endsWith("/stream") || url.pathname.endsWith("/stream.view");
    const isDownloadPath = url.pathname.endsWith("/download") || url.pathname.endsWith("/download.view");
    const isHLSPath = url.pathname.endsWith("/hls.m3u8");

    if (!isStreamPath && !isDownloadPath && !isHLSPath) {
      return fetch(request);
    }

    // Construir la URL de destino al origen lampara (sound.listo.click)
    const originHost = env.ORIGIN_HOST || "sound.listo.click";
    const originTargetURL = new URL(request.url);
    originTargetURL.hostname = originHost;
    originTargetURL.protocol = "https:";
    originTargetURL.port = "";

    // Preguntamos al origen (lampara) usando un header especial para que solo resuelva metadatos
    const resolveReqHeaders = new Headers(request.headers);
    resolveReqHeaders.set("X-Substream-Resolve-Only", "1");
    resolveReqHeaders.set("X-Worker-Proxy", "1");
    resolveReqHeaders.set("X-Substream-Worker-Secret", env.WORKER_SECRET || "2541a09b5b6effe619883446e7f5d33a6ffc7b9f15908fa567f03c43a8077731");
    resolveReqHeaders.set("Host", originHost);

    let resolveResp;
    try {
      resolveResp = await fetch(originTargetURL.toString(), {
        method: request.method,
        headers: resolveReqHeaders,
        redirect: "manual"
      });
    } catch (err) {
      console.error("Error contactando origin lampara:", err);
      return new Response("Error al conectar con Substream origin", { status: 502 });
    }

    // Caso A: El origen devolvió un 302 Redirect directo a la CDN upstream
    if (resolveResp.status === 302 || resolveResp.status === 301 || resolveResp.status === 307) {
      const upstreamURL = resolveResp.headers.get("Location");
      if (upstreamURL) {
        return proxyUpstream(upstreamURL, request, env, ctx);
      }
    }

    // Caso B: El origen devolvió JSON con los metadatos resueltos (X-Substream-Resolve-Only)
    const contentType = resolveResp.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      try {
        const data = await resolveResp.clone().json();
        if (data && data.upstream_url) {
          if (data.is_download) {
            return proxyDownloadWithTags(data.upstream_url, request, env, ctx, data);
          }
          // HLS/fMP4: el manifiesto NO se puede entregar a /stream tal cual.
          // ExoPlayer (Tempus, Symfonium, DSub) usa ProgressiveMediaSource sobre
          // /stream y no sabe decodificar un playlist M3U8, así que devolverle el
          // manifest rompe la reproducción. El stitching fMP4 sólo puede hacerlo
          // el origen (necesita descargar los segmentos), así que re-pedimos el
          // stream sin las cabeceras de resolución y dejamos que lampara cos(a)
          // los segmentos en un MP4 progresivo.
          // Sólo se fuerza el stitch cuando el ORIGEN dice que este cliente no puede
          // consumir upstream_url tal cual (p. ej. Tempus/Symfonium/DSub con HLS).
          // Los clientes con HLS nativo (Supersonic) reciben is_hls=true pero
          // direct_playable=true y deben seguir recibiendo el manifiesto.
          if (data.direct_playable === false) {
            return fetchOriginStitched(originTargetURL, request, env, ctx, originHost);
          }
          return proxyUpstream(data.upstream_url, request, env, ctx, data);
        }
      } catch (e) {
        // Fallback a respuesta original
      }
    }

    // Caso C: Si no se pudo resolver como upstream CDN (ej: archivo local o stream HLS especial de lampara),
    // devolvemos la respuesta de lampara tal cual (ej. la descarga ya remuxeada por lampara)
    return resolveResp;
  }
};

// Re-pide /stream al origen SIN las cabeceras de resolución para que lampara
// ejecute el stitch HLS y devuelva un MP4 progresivo (fMP4 cosido) que
// ExoPlayer sí puede decodificar. El Worker reenvía el cuerpo tal cual.
async function fetchOriginStitched(originTargetURL, request, env, ctx, originHost) {
  const headers = new Headers();
  // Sólo lo mínimo imprescindible: la query ya lleva la autenticación Subsonic.
  const passThrough = ["range", "user-agent", "accept"];
  for (const h of passThrough) {
    const v = request.headers.get(h);
    if (v) headers.set(h, v);
  }
  headers.set("Host", originHost);
  const clientIP = request.headers.get("cf-connecting-ip") || request.headers.get("x-real-ip");
  if (clientIP) {
    headers.set("X-Forwarded-For", clientIP);
    headers.set("X-Real-IP", clientIP);
  }

  let originResp;
  try {
    originResp = await fetch(originTargetURL.toString(), {
      method: request.method,
      headers,
      redirect: "manual"
    });
  } catch (err) {
    console.error("Error re-pidiendo stream stitched al origin:", err);
    return new Response("Error al conectar con Substream origin", { status: 502 });
  }

  // El origen puede seguir contestando 302 a un CDN progresivo (unwrap exitoso).
  if (originResp.status === 301 || originResp.status === 302 || originResp.status === 307) {
    const location = originResp.headers.get("Location");
    if (location) {
      return proxyUpstream(location, request, env, ctx);
    }
  }

  const outHeaders = new Headers();
  copyHeaders(originResp.headers, outHeaders);
  if (!outHeaders.has("content-type")) {
    outHeaders.set("content-type", "audio/mp4");
  }
  // El origen decide: para ExoPlayer (Tempus/Symfonium/DSub) convierte el Range
  // en timeOffset y responde 206, así que su accept-ranges debe respetarse.
  // Si no lo declara, el stream cosido no es seekable por bytes.
  if (!outHeaders.has("accept-ranges")) {
    outHeaders.set("accept-ranges", "none");
  }
  outHeaders.set("X-Stream-Proxy-By", "Cloudflare-Worker-Substream-Stitched");

  return new Response(originResp.body, {
    status: originResp.status,
    headers: outHeaders
  });
}

async function proxyUpstream(upstreamURL, request, env, ctx, meta = null) {
  const reqHeaders = new Headers();

  const range = request.headers.get("range");
  if (range) {
    reqHeaders.set("Range", range);
  }

  const isYouTube = upstreamURL.includes("googlevideo.com");
  const clientIP = request.headers.get("cf-connecting-ip") || request.headers.get("x-real-ip");
  if (clientIP && !isYouTube) {
    reqHeaders.set("X-Forwarded-For", clientIP);
    reqHeaders.set("X-Real-IP", clientIP);
  }

  reqHeaders.set("User-Agent", request.headers.get("user-agent") || "Substream-Worker/1.0");

  let upstreamResp = null;
  let lastError = null;

  for (let attempt = 0; attempt < RETRY_ATTEMPTS; attempt++) {
    try {
      if (attempt > 0) {
        await new Promise(r => setTimeout(r, 100 * Math.pow(2, attempt)));
      }

      upstreamResp = await fetch(upstreamURL, {
        method: request.method === "HEAD" ? "HEAD" : "GET",
        headers: reqHeaders,
        redirect: "follow"
      });

      if (upstreamResp.ok || upstreamResp.status === 206) {
        break;
      }
    } catch (err) {
      lastError = err;
    }
  }

  if (!upstreamResp || (!upstreamResp.ok && upstreamResp.status !== 206)) {
    return new Response(`Error al obtener stream del upstream CDN: ${lastError || (upstreamResp && upstreamResp.status)}`, {
      status: 502,
      headers: { "Content-Type": "text/plain" }
    });
  }

  const outHeaders = new Headers();
  copyHeaders(upstreamResp.headers, outHeaders);

  if (!outHeaders.has("content-type")) {
    if (upstreamURL.includes(".flac")) {
      outHeaders.set("content-type", "audio/flac");
    } else if (upstreamURL.includes(".mp3")) {
      outHeaders.set("content-type", "audio/mpeg");
    } else {
      outHeaders.set("content-type", "audio/mp4");
    }
  }

  if (meta && meta.is_stitched) {
    outHeaders.set("accept-ranges", "none");
  } else if (!outHeaders.has("accept-ranges")) {
    outHeaders.set("accept-ranges", "bytes");
  }

  outHeaders.set("X-Stream-Proxy-By", "Cloudflare-Worker-Substream");

  return new Response(upstreamResp.body, {
    status: upstreamResp.status,
    headers: outHeaders
  });
}

// Inyección al vuelo de Vorbis Comments y Cover Art para FLAC en descargas
async function proxyDownloadWithTags(upstreamURL, request, env, ctx, data) {
  const reqHeaders = new Headers();
  reqHeaders.set("User-Agent", request.headers.get("user-agent") || "Substream-Worker/1.0");

  const upstreamResp = await fetch(upstreamURL, {
    method: "GET",
    headers: reqHeaders,
    redirect: "follow"
  });

  if (!upstreamResp.ok) {
    return new Response("Error al descargar desde upstream CDN", { status: 502 });
  }

  const outHeaders = new Headers();
  outHeaders.set("Content-Type", data.content_type || "audio/flac");
  outHeaders.set("Content-Disposition", `attachment; filename="${data.filename}"`);
  outHeaders.set("Cache-Control", "no-cache");
  outHeaders.set("X-Stream-Proxy-By", "Cloudflare-Worker-Substream-Tagged");

  // Si no es FLAC o no hay metadatos, entregar el cuerpo directamente
  if (!data.filename.endsWith(".flac") || !data.metadata) {
    return new Response(upstreamResp.body, {
      status: 200,
      headers: outHeaders
    });
  }

  // Descargar la portada si está especificada
  let coverBytes = null;
  if (data.metadata.CoverURL) {
    try {
      const coverResp = await fetch(data.metadata.CoverURL);
      if (coverResp.ok) {
        coverBytes = new Uint8Array(await coverResp.arrayBuffer());
      }
    } catch (e) {
      console.warn("No se pudo obtener la portada:", e);
    }
  }

  // Empaquetar stream con tags Vorbis y Cover Art
  const taggedStream = buildTaggedFlacStream(upstreamResp.body, data.metadata, coverBytes);

  return new Response(taggedStream, {
    status: 200,
    headers: outHeaders
  });
}

function buildTaggedFlacStream(rawBody, meta, coverBytes) {
  const reader = rawBody.getReader();

  return new ReadableStream({
    async start(controller) {
      let buffer = new Uint8Array(0);

      function concat(a, b) {
        const c = new Uint8Array(a.length + b.length);
        c.set(a, 0);
        c.set(b, a.length);
        return c;
      }

      // Leer hasta tener al menos los bloques de cabecera iniciales (STREAMINFO)
      while (buffer.length < 42) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer = concat(buffer, value);
      }

      // Verificar fLaC magic
      if (buffer.length < 4 || String.fromCharCode(...buffer.slice(0, 4)) !== "fLaC") {
        controller.enqueue(buffer);
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          controller.enqueue(value);
        }
        controller.close();
        return;
      }

      // El primer bloque debe ser STREAMINFO (longitud 34 bytes)
      let offset = 4;
      const originalBlocks = [];

      while (offset + 4 <= buffer.length) {
        const header = buffer.slice(offset, offset + 4);
        const isLast = (header[0] & 0x80) !== 0;
        const blockType = header[0] & 0x7F;
        const blockLength = (header[1] << 16) | (header[2] << 8) | header[3];

        while (buffer.length < offset + 4 + blockLength) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer = concat(buffer, value);
        }

        if (buffer.length < offset + 4 + blockLength) break;

        const blockData = buffer.slice(offset + 4, offset + 4 + blockLength);
        offset += 4 + blockLength;

        // Conservar todos los bloques excepto VORBIS_COMMENT (4) y PICTURE (6)
        if (blockType !== 4 && blockType !== 6) {
          originalBlocks.push({ type: blockType, data: blockData });
        }

        if (isLast) break;
      }

      // Generar nuevo bloque Vorbis Comment
      const vorbisBlock = makeVorbisCommentBlock(meta);
      // Generar bloque Picture si hay carátula
      const pictureBlock = coverBytes ? makePictureBlock(coverBytes) : null;

      // Ensamblar bloques
      const blocksToSend = [...originalBlocks];
      if (vorbisBlock) blocksToSend.push({ type: 4, data: vorbisBlock });
      if (pictureBlock) blocksToSend.push({ type: 6, data: pictureBlock });

      // Enviar fLaC header
      controller.enqueue(new TextEncoder().encode("fLaC"));

      for (let i = 0; i < blocksToSend.length; i++) {
        const b = blocksToSend[i];
        const isLast = i === blocksToSend.length - 1;
        const header = new Uint8Array(4);
        header[0] = (isLast ? 0x80 : 0x00) | (b.type & 0x7F);
        header[1] = (b.data.length >> 16) & 0xFF;
        header[2] = (b.data.length >> 8) & 0xFF;
        header[3] = b.data.length & 0xFF;

        controller.enqueue(header);
        controller.enqueue(b.data);
      }

      // Enviar el resto del audio frames
      if (offset < buffer.length) {
        controller.enqueue(buffer.slice(offset));
      }

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        controller.enqueue(value);
      }

      controller.close();
    }
  });
}

function makeVorbisCommentBlock(meta) {
  const vendor = "Substream-Cloudflare-Worker";
  const tags = [];

  if (meta.Title) tags.push(`TITLE=${meta.Title}`);
  if (meta.Artist) tags.push(`ARTIST=${meta.Artist}`);
  if (meta.Album) tags.push(`ALBUM=${meta.Album}`);
  if (meta.AlbumArtist || meta.Artist) tags.push(`ALBUMARTIST=${meta.AlbumArtist || meta.Artist}`);
  if (meta.TrackNumber > 0) tags.push(`TRACKNUMBER=${meta.TrackNumber}`);
  if (meta.DiscNumber > 0) tags.push(`DISCNUMBER=${meta.DiscNumber}`);
  if (meta.Date) tags.push(`DATE=${meta.Date}`);
  if (meta.Year) tags.push(`YEAR=${meta.Year}`);
  if (meta.ISRC) tags.push(`ISRC=${meta.ISRC}`);
  if (meta.Comment) tags.push(`COMMENT=${meta.Comment}`);

  const encoder = new TextEncoder();
  const vendorBytes = encoder.encode(vendor);

  let totalSize = 4 + vendorBytes.length + 4;
  const encodedTags = tags.map(t => {
    const b = encoder.encode(t);
    totalSize += 4 + b.length;
    return b;
  });

  const out = new Uint8Array(totalSize);
  const view = new DataView(out.buffer);
  let pos = 0;

  // vendor length (LE)
  view.setUint32(pos, vendorBytes.length, true);
  pos += 4;
  out.set(vendorBytes, pos);
  pos += vendorBytes.length;

  // tags count (LE)
  view.setUint32(pos, encodedTags.length, true);
  pos += 4;

  for (const t of encodedTags) {
    view.setUint32(pos, t.length, true);
    pos += 4;
    out.set(t, pos);
    pos += t.length;
  }

  return out;
}

function makePictureBlock(imgData) {
  const mime = "image/jpeg";
  const desc = "Cover";
  const encoder = new TextEncoder();
  const mimeBytes = encoder.encode(mime);
  const descBytes = encoder.encode(desc);

  const totalSize = 4 + 4 + mimeBytes.length + 4 + descBytes.length + 16 + 4 + imgData.length;
  const out = new Uint8Array(totalSize);
  const view = new DataView(out.buffer);
  let pos = 0;

  // picture type: 3 = Cover (front) (BE)
  view.setUint32(pos, 3, false);
  pos += 4;

  // MIME length + data
  view.setUint32(pos, mimeBytes.length, false);
  pos += 4;
  out.set(mimeBytes, pos);
  pos += mimeBytes.length;

  // Description length + data
  view.setUint32(pos, descBytes.length, false);
  pos += 4;
  out.set(descBytes, pos);
  pos += descBytes.length;

  // Width, Height, Depth, Indexed colors
  view.setUint32(pos, 1280, false); pos += 4;
  view.setUint32(pos, 1280, false); pos += 4;
  view.setUint32(pos, 24, false); pos += 4;
  view.setUint32(pos, 0, false); pos += 4;

  // Image data length + payload
  view.setUint32(pos, imgData.length, false);
  pos += 4;
  out.set(imgData, pos);

  return out;
}
