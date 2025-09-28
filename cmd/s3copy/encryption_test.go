package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonceManager(t *testing.T) {
	t.Run("NewNonceManager", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)
		assert.NotNil(t, nm)
		assert.NotNil(t, nm.baseNonce)
		assert.Equal(t, uint64(0), nm.counter)
	})

	t.Run("NextNonce_Sequential", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)

		nonce1 := nm.NextNonce()
		assert.Equal(t, uint64(1), nm.counter)
		assert.Len(t, nonce1, 12) // ChaCha20 nonce size

		nonce2 := nm.NextNonce()
		assert.Equal(t, uint64(2), nm.counter)
		assert.Len(t, nonce2, 12)

		assert.NotEqual(t, nonce1, nonce2)
	})

	t.Run("GetBaseNonce", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)

		baseNonce := nm.GetBaseNonce()
		assert.Len(t, baseNonce, 12)
		assert.Equal(t, nm.baseNonce, baseNonce)
	})

	t.Run("Reset", func(t *testing.T) {
		nm, err := NewNonceManager()
		require.NoError(t, err)

		_ = nm.NextNonce()
		_ = nm.NextNonce()
		assert.Equal(t, uint64(2), nm.counter)

		nm.Reset()
		assert.Equal(t, uint64(0), nm.counter)
	})
}

func TestEncryptDecryptStream(t *testing.T) {
	password = "testpassword123"

	t.Run("round trip encryption", func(t *testing.T) {
		originalData := []byte("This is a test message for encryption and decryption.")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		require.NoError(t, err)

		assert.Equal(t, originalData, decrypted.Bytes())
	})

	t.Run("wrong password", func(t *testing.T) {
		originalData := []byte("Test data")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		password = "wrongpassword"
		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decryption failed")
	})

	t.Run("empty data", func(t *testing.T) {
		originalData := []byte("")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		require.NoError(t, err)

		decryptedData := decrypted.Bytes()
		if decryptedData == nil {
			decryptedData = []byte{}
		}
		assert.Equal(t, originalData, decryptedData)
	})

	t.Run("large data", func(t *testing.T) {
		originalData := make([]byte, 2*1024*1024)
		_, err := rand.Read(originalData)
		require.NoError(t, err)

		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err = encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		decrypted := &bytes.Buffer{}
		err = decryptStreamFromReader(decrypted, encryptedReader)
		require.NoError(t, err)

		assert.Equal(t, originalData, decrypted.Bytes())
	})

	t.Run("encrypt write error", func(t *testing.T) {
		originalData := []byte("test data")
		input := bytes.NewReader(originalData)

		failingWriter := &failingWriter{}
		err := encryptStream(failingWriter, input)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write")
	})

	t.Run("decrypt write error", func(t *testing.T) {
		originalData := []byte("test data")
		input := bytes.NewReader(originalData)

		encrypted := &bytes.Buffer{}
		err := encryptStream(encrypted, input)
		require.NoError(t, err)

		encryptedReader := bytes.NewReader(encrypted.Bytes())
		failingWriter := &failingWriter{}
		err = decryptStreamFromReader(failingWriter, encryptedReader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write decrypted data")
	})

	t.Run("decrypt read error", func(t *testing.T) {
		incompleteData := []byte("incomplete")
		encryptedReader := bytes.NewReader(incompleteData)
		decrypted := &bytes.Buffer{}
		err := decryptStreamFromReader(decrypted, encryptedReader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read encryption header")
	})
}

// failingWriter is a writer that always fails
type failingWriter struct{}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("write failed")
}
