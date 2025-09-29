package com.itextos.beacon.commonlib.pwdencryption;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.HexFormat;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public final class Aes256Encrypt {
    // Modern cryptographic constants
    private static final String GCM_ALGORITHM = "AES/GCM/NoPadding";
    private static final String CBC_ALGORITHM = "AES/CBC/PKCS5Padding";
    private static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
    
    // GCM parameters
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_IV_LENGTH = 12;
    
    // Legacy CBC parameters
    private static final int CBC_IV_LENGTH = 16;
    private static final int LEGACY_SALT_LENGTH = 10;
    
    // Modern parameters
    private static final int MODERN_SALT_LENGTH = 16;
    private static final int ITERATION_COUNT = 210_000;
    private static final int KEY_LENGTH = 256;
    
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private Aes256Encrypt() {
        throw new AssertionError("Cannot instantiate utility class");
    }

    /**
     * Decrypts data with backward compatibility for legacy CBC format
     */
    public static String decrypt(String encryptedText, String secretKey) throws Exception {
        if (encryptedText == null || encryptedText.isBlank()) {
            return encryptedText;
        }
        
        try {
            // First try modern GCM format
            return decryptGCM(encryptedText, secretKey);
        } catch (SecurityException e) {
            // If GCM fails, try legacy CBC format
            try {
                return decryptLegacyCBC(encryptedText, secretKey);
            } catch (Exception legacyException) {
                // Combine both error messages for debugging
                throw new SecurityException(
                    "Decryption failed with both GCM and legacy CBC modes. " +
                    "GCM error: " + e.getMessage() + ", " +
                    "CBC error: " + legacyException.getMessage(), e);
            }
        }
    }

    /**
     * Modern GCM decryption
     */
    private static String decryptGCM(String encryptedText, String secretKey) throws Exception {
        final byte[] decoded = Base64.getDecoder().decode(encryptedText);
        
        if (decoded.length < (GCM_IV_LENGTH + MODERN_SALT_LENGTH + 1)) {
            throw new SecurityException("Input data too short to contain an expected tag length of 16 bytes");
        }
        
        final ByteBuffer buffer = ByteBuffer.wrap(decoded);
        
        final byte[] iv = new byte[GCM_IV_LENGTH];
        buffer.get(iv);
        
        final byte[] salt = new byte[MODERN_SALT_LENGTH];
        buffer.get(salt);
        
        final byte[] cipherText = new byte[buffer.remaining()];
        buffer.get(cipherText);

        final Cipher cipher = Cipher.getInstance(GCM_ALGORITHM);
        final SecretKey key = deriveKey(secretKey, salt);
        final GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        
        cipher.init(Cipher.DECRYPT_MODE, key, gcmSpec);
        final byte[] plainText = cipher.doFinal(cipherText);
        
        return new String(plainText, StandardCharsets.UTF_8);
    }

    /**
     * Legacy CBC decryption for backward compatibility
     */
    private static String decryptLegacyCBC(String encryptedText, String secretKey) throws Exception {
        final byte[] decoded = Base64.getDecoder().decode(encryptedText);
        final ByteBuffer buffer = ByteBuffer.wrap(decoded);
        
        final byte[] initialVectorByte = new byte[CBC_IV_LENGTH];
        buffer.get(initialVectorByte);

        final byte[] saltByte = new byte[LEGACY_SALT_LENGTH];
        buffer.get(saltByte);

        final byte[] cipherText = new byte[buffer.remaining()];
        buffer.get(cipherText);

        final Cipher cipher = Cipher.getInstance(CBC_ALGORITHM);
        final SecretKey secret = deriveKeyLegacy(secretKey, saltByte);
        cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(initialVectorByte));
        
        final byte[] plainText = cipher.doFinal(cipherText);
        return new String(plainText, StandardCharsets.UTF_8);
    }

    /**
     * Encrypts using modern GCM format by default
     */
    public static EncryptedObject encrypt(String plainText, String secretKey) throws Exception {
        return encryptGCM(plainText, secretKey);
    }

    /**
     * Modern GCM encryption
     */
    private static EncryptedObject encryptGCM(String plainText, String secretKey) throws Exception {
        final byte[] iv = generateSecureRandomBytes(GCM_IV_LENGTH);
        final byte[] salt = generateSecureRandomBytes(MODERN_SALT_LENGTH);
        
        final byte[] encryptedBytes = encryptGCMInternal(plainText, secretKey, salt, iv);
        
        final ByteBuffer buffer = ByteBuffer.allocate(iv.length + salt.length + encryptedBytes.length);
        buffer.put(iv).put(salt).put(encryptedBytes);
        
        final String encryptedWithMetadata = Base64.getEncoder().encodeToString(buffer.array());
        return new EncryptedObject(plainText, encryptedWithMetadata);
    }

    /**
     * Legacy CBC encryption (if needed for migration)
     */
    public static EncryptedObject encryptLegacyCBC(String plainText, String secretKey) throws Exception {
        final byte[] intialVectorBytes = generateSecureRandomBytes(CBC_IV_LENGTH);
        final byte[] saltBytes = generateSecureRandomBytes(LEGACY_SALT_LENGTH);
        final byte[] encryptedBytes = encryptLegacyCBCInternal(plainText, secretKey, saltBytes, intialVectorBytes);

        final ByteBuffer byteBuffer = ByteBuffer.allocate(
            intialVectorBytes.length + saltBytes.length + encryptedBytes.length);
        byteBuffer.put(intialVectorBytes).put(saltBytes).put(encryptedBytes);

        final String encryptedWithIvAndSalt = Base64.getEncoder().encodeToString(byteBuffer.array());
        return new EncryptedObject(plainText, encryptedWithIvAndSalt);
    }

    private static byte[] encryptGCMInternal(String plainText, String secretKey, 
                                           byte[] salt, byte[] iv) throws Exception {
        final SecretKey key = deriveKey(secretKey, salt);
        final Cipher cipher = Cipher.getInstance(GCM_ALGORITHM);
        final GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        
        cipher.init(Cipher.ENCRYPT_MODE, key, gcmSpec);
        return cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] encryptLegacyCBCInternal(String plainText, String secretKey,
                                                 byte[] salt, byte[] iv) throws Exception {
        final IvParameterSpec ivspec = new IvParameterSpec(iv);
        final SecretKey secret = deriveKeyLegacy(secretKey, salt);
        final Cipher cipher = Cipher.getInstance(CBC_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secret, ivspec);
        return cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Modern key derivation
     */
    private static SecretKey deriveKey(String base64SecretKey, byte[] salt) 
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        
        final String decodedKey = new String(Base64.getDecoder().decode(base64SecretKey), 
                                           StandardCharsets.UTF_8);
        
        final SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
        final KeySpec spec = new PBEKeySpec(
            decodedKey.toCharArray(), 
            salt, 
            ITERATION_COUNT, 
            KEY_LENGTH
        );
        
        final byte[] keyBytes = factory.generateSecret(spec).getEncoded();
        return new SecretKeySpec(keyBytes, "AES");
    }

    /**
     * Legacy key derivation (for backward compatibility)
     */
    private static SecretKey deriveKeyLegacy(String base64SecretKey, byte[] saltByte)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        
        final String decodedKey = new String(Base64.getDecoder().decode(base64SecretKey));
        final SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
        final KeySpec spec = new PBEKeySpec(decodedKey.toCharArray(), saltByte, 65536, 256);
        return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
    }

    private static byte[] generateSecureRandomBytes(int length) {
        final byte[] bytes = new byte[length];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * Detects if encrypted data is in legacy format
     */
    public static boolean isLegacyFormat(String encryptedText) {
        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedText);
            // Legacy format has 16-byte IV + 10-byte salt
            return decoded.length >= (CBC_IV_LENGTH + LEGACY_SALT_LENGTH + 1);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Migration utility to convert legacy CBC data to modern GCM format
     */
    public static String migrateToGCM(String legacyEncryptedText, String secretKey) throws Exception {
        String decrypted = decryptLegacyCBC(legacyEncryptedText, secretKey);
        EncryptedObject modernEncrypted = encryptGCM(decrypted, secretKey);
        return modernEncrypted.getEncryptedWithIvAndSalt();
    }

    // Test method
    public static void main(String[] args) throws Exception {
        String testText = "cfguser_password";
        String key = "S3VtYXJhcGFuZGlhbmlUZXh0b3NTdW1hdGhp"; // DB_PASSWORD_KEY
        
        // Test backward compatibility
        System.out.println("Testing backward compatibility...");
        
        // Create legacy encrypted data for testing
        EncryptedObject legacyEncrypted = encryptLegacyCBC(testText, key);
        System.out.println("Legacy format: " + isLegacyFormat(legacyEncrypted.getEncryptedWithIvAndSalt()));
        
        // Decrypt using new method (should work with both)
        String decrypted = decrypt(legacyEncrypted.getEncryptedWithIvAndSalt(), key);
        System.out.println("Decryption successful: " + testText.equals(decrypted));
        
        // Test modern format
        EncryptedObject modernEncrypted = encryptGCM(testText, key);
        System.out.println("Modern format: " + !isLegacyFormat(modernEncrypted.getEncryptedWithIvAndSalt()));
        
        decrypted = decrypt(modernEncrypted.getEncryptedWithIvAndSalt(), key);
        System.out.println("Modern decryption successful: " + testText.equals(decrypted));
    }
}