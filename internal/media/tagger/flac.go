package tagger

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
)

// TagFLAC reads a raw FLAC stream from r, injects Vorbis metadata tags and
// an optional front cover picture block, and writes the complete tagged FLAC stream to w.
func TagFLAC(ctx context.Context, r io.Reader, w io.Writer, meta *Metadata, client *http.Client) error {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("read flac header: %w", err)
	}

	// If stream does not start with fLaC, write header and copy remainder raw
	if string(header) != "fLaC" {
		if _, err := w.Write(header); err != nil {
			return err
		}
		_, err := io.Copy(w, r)
		return err
	}

	if _, err := w.Write(header); err != nil {
		return err
	}

	var blocks [][]byte

	// Read all original FLAC metadata blocks
	for {
		blockHead := make([]byte, 4)
		if _, err := io.ReadFull(r, blockHead); err != nil {
			return fmt.Errorf("read flac metadata block header: %w", err)
		}

		last := blockHead[0]&0x80 != 0
		typ := blockHead[0] & 0x7F
		length := int(blockHead[1])<<16 | int(blockHead[2])<<8 | int(blockHead[3])

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return fmt.Errorf("read flac metadata block data: %w", err)
		}

		// Keep existing blocks except Vorbis Comment (4) and Picture (6)
		if typ != 4 && typ != 6 {
			blocks = append(blocks, append(blockHead, data...))
		}

		if last {
			break
		}
	}

	// Inject new Vorbis comments and Picture block
	if meta != nil {
		commentBlock := makeVorbisComment(meta)
		blocks = append(blocks, commentBlock)

		coverData := meta.FetchCover(ctx, client)
		if len(coverData) > 0 {
			picBlock := makePictureBlock(coverData)
			if picBlock != nil {
				blocks = append(blocks, picBlock)
			}
		}
	}

	// Write reconstructed blocks with correct last-block flags
	for i, b := range blocks {
		if i == len(blocks)-1 {
			b[0] |= 0x80 // Set last block flag
		} else {
			b[0] &= 0x7F // Clear last block flag
		}
		if _, err := w.Write(b); err != nil {
			return fmt.Errorf("write flac metadata block: %w", err)
		}
	}

	// Stream audio frames
	_, err := io.Copy(w, r)
	return err
}

func makeVorbisComment(meta *Metadata) []byte {
	vendor := "Substream"

	tags := make(map[string]string)
	if meta.Title != "" {
		tags["TITLE"] = meta.Title
	}
	if meta.Artist != "" {
		tags["ARTIST"] = meta.Artist
	}
	if meta.Album != "" {
		tags["ALBUM"] = meta.Album
	}
	if meta.AlbumArtist != "" {
		tags["ALBUMARTIST"] = meta.AlbumArtist
	} else if meta.Artist != "" {
		tags["ALBUMARTIST"] = meta.Artist
	}
	if meta.TrackNumber > 0 {
		tags["TRACKNUMBER"] = fmt.Sprint(meta.TrackNumber)
	}
	if meta.DiscNumber > 0 {
		tags["DISCNUMBER"] = fmt.Sprint(meta.DiscNumber)
	}
	if meta.Date != "" {
		tags["DATE"] = meta.Date
	}
	if meta.Year != "" {
		tags["YEAR"] = meta.Year
	} else if len(meta.Date) >= 4 {
		tags["YEAR"] = meta.Date[:4]
	}
	if meta.ISRC != "" {
		tags["ISRC"] = meta.ISRC
	}
	if meta.Comment != "" {
		tags["COMMENT"] = meta.Comment
	}

	buf := new(bytes.Buffer)
	// vendor length (4 bytes little-endian)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(vendor)))
	buf.WriteString(vendor)

	// tag count (4 bytes little-endian)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(tags)))
	for k, v := range tags {
		entry := k + "=" + v
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(entry)))
		buf.WriteString(entry)
	}

	data := buf.Bytes()
	header := make([]byte, 4)
	header[0] = 4 // type 4: VORBIS_COMMENT
	header[1] = byte(len(data) >> 16)
	header[2] = byte(len(data) >> 8)
	header[3] = byte(len(data))

	return append(header, data...)
}

func makePictureBlock(imgData []byte) []byte {
	if len(imgData) == 0 {
		return nil
	}

	buf := new(bytes.Buffer)
	// Picture type: 3 = Cover (front)
	_ = binary.Write(buf, binary.BigEndian, uint32(3))

	mime := "image/jpeg"
	if bytes.HasPrefix(imgData, []byte("\x89PNG")) {
		mime = "image/png"
	}
	_ = binary.Write(buf, binary.BigEndian, uint32(len(mime)))
	buf.WriteString(mime)

	desc := "Cover"
	_ = binary.Write(buf, binary.BigEndian, uint32(len(desc)))
	buf.WriteString(desc)

	// width, height, color depth, indexed color count
	_ = binary.Write(buf, binary.BigEndian, uint32(1280))
	_ = binary.Write(buf, binary.BigEndian, uint32(1280))
	_ = binary.Write(buf, binary.BigEndian, uint32(24))
	_ = binary.Write(buf, binary.BigEndian, uint32(0))

	// Data length + payload
	_ = binary.Write(buf, binary.BigEndian, uint32(len(imgData)))
	buf.Write(imgData)

	data := buf.Bytes()
	header := make([]byte, 4)
	header[0] = 6 // type 6: PICTURE
	header[1] = byte(len(data) >> 16)
	header[2] = byte(len(data) >> 8)
	header[3] = byte(len(data))

	return append(header, data...)
}
