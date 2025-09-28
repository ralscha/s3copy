package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

type EncryptionParams struct {
	Salt  []byte
	Nonce []byte
}

// NonceManager handles secure nonce generation for chunked encryption
type NonceManager struct {
	baseNonce []byte
	counter   uint64
}

// NewNonceManager creates a new nonce manager with a random base nonce
func NewNonceManager() (*NonceManager, error) {
	baseNonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := rand.Read(baseNonce); err != nil {
		return nil, fmt.Errorf("failed to generate base nonce: %v", err)
	}

	return &NonceManager{
		baseNonce: baseNonce,
		counter:   0,
	}, nil
}

// NextNonce returns the next nonce in the sequence
func (nm *NonceManager) NextNonce() []byte {
	nonce := make([]byte, chacha20poly1305.NonceSize)
	copy(nonce, nm.baseNonce)

	binary.BigEndian.PutUint64(nonce[chacha20poly1305.NonceSize-8:], nm.counter)
	nm.counter++

	return nonce
}

// GetBaseNonce returns the base nonce for storage/transmission
func (nm *NonceManager) GetBaseNonce() []byte {
	return nm.baseNonce
}

// Reset resets the counter (used when starting decryption)
func (nm *NonceManager) Reset() {
	nm.counter = 0
}

func encryptStream(writer io.Writer, reader io.Reader) error {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("failed to generate salt: %v", err)
	}

	nonceManager, err := NewNonceManager()
	if err != nil {
		return err
	}

	if _, err := writer.Write(salt); err != nil {
		return fmt.Errorf("failed to write salt: %v", err)
	}
	if _, err := writer.Write(nonceManager.GetBaseNonce()); err != nil {
		return fmt.Errorf("failed to write base nonce: %v", err)
	}

	key := argon2.IDKey([]byte(password), salt, 3, 64*1024, 4, 32)

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create AEAD cipher: %v", err)
	}

	buf := make([]byte, 1024*1024) // 1MB chunks
	chunkCount := uint64(0)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			chunkNonce := nonceManager.NextNonce()
			encryptedChunk := aead.Seal(nil, chunkNonce, buf[:n], nil)
			chunkSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(chunkSizeBytes, uint32(len(encryptedChunk)))

			if _, writeErr := writer.Write(chunkSizeBytes); writeErr != nil {
				return fmt.Errorf("failed to write chunk size: %v", writeErr)
			}
			if _, writeErr := writer.Write(encryptedChunk); writeErr != nil {
				return fmt.Errorf("failed to write encrypted chunk: %v", writeErr)
			}

			chunkCount++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read from source: %v", err)
		}
	}

	return nil
}

func decryptStreamFromReader(writer io.Writer, reader io.Reader) error {
	header := make([]byte, 44) // 32 (salt) + 12 (base nonce)
	if _, err := io.ReadFull(reader, header); err != nil {
		return fmt.Errorf("failed to read encryption header: %v", err)
	}

	salt := header[:32]
	baseNonce := header[32:44]

	key := argon2.IDKey([]byte(password), salt, 3, 64*1024, 4, 32)

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return fmt.Errorf("failed to create AEAD cipher: %v", err)
	}

	nonceManager := &NonceManager{
		baseNonce: make([]byte, chacha20poly1305.NonceSize),
		counter:   0,
	}
	copy(nonceManager.baseNonce, baseNonce)

	for {
		chunkSizeBytes := make([]byte, 4)
		if _, err := io.ReadFull(reader, chunkSizeBytes); err != nil {
			if err == io.EOF {
				break // Normal end of stream
			}
			return fmt.Errorf("failed to read chunk size: %v", err)
		}

		chunkSize := binary.BigEndian.Uint32(chunkSizeBytes)

		encryptedChunk := make([]byte, chunkSize)
		if _, err := io.ReadFull(reader, encryptedChunk); err != nil {
			return fmt.Errorf("failed to read encrypted chunk: %v", err)
		}

		chunkNonce := nonceManager.NextNonce()
		plaintext, err := aead.Open(nil, chunkNonce, encryptedChunk, nil)
		if err != nil {
			return fmt.Errorf("decryption failed (wrong password or corrupted data?): %v", err)
		}

		if _, err := writer.Write(plaintext); err != nil {
			return fmt.Errorf("failed to write decrypted data: %v", err)
		}
	}

	return nil
}
