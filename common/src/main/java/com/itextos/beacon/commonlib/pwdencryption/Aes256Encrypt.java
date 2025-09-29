package com.itextos.beacon.commonlib.pwdencryption;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public final class Aes256Encrypt {
    // Modern cryptographic constants
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
    private static final int GCM_TAG_LENGTH = 128; // bits
    private static final int GCM_IV_LENGTH = 12;   // bytes (recommended for GCM)
    private static final int SALT_LENGTH = 16;     // bytes
    private static final int ITERATION_COUNT = 210_000; // OWASP recommended 2024
    private static final int KEY_LENGTH = 256;     // bits
    
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    // Private constructor to prevent instantiation
    private Aes256Encrypt() {
        throw new AssertionError("Cannot instantiate utility class");
    }

    /**
     * Decrypts a string encrypted with AES-256-GCM.
     *
     * @param encryptedText Base64 encoded string containing IV, salt and ciphertext
     * @param secretKey     Base64 encoded secret key
     * @return Decrypted plaintext
     * @throws Exception if decryption fails
     */
    public static String decrypt(String encryptedText, String secretKey) throws Exception {
        try {
            final byte[] decoded = Base64.getDecoder().decode(encryptedText);
            final ByteBuffer buffer = ByteBuffer.wrap(decoded);
            
            // Extract components from the encoded data
            final byte[] iv = new byte[GCM_IV_LENGTH];
            buffer.get(iv);
            
            final byte[] salt = new byte[SALT_LENGTH];
            buffer.get(salt);
            
            final byte[] cipherText = new byte[buffer.remaining()];
            buffer.get(cipherText);

            // Initialize cipher with GCM
            final Cipher cipher = Cipher.getInstance(ALGORITHM);
            final SecretKey key = deriveKey(secretKey, salt);
            final GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            
            cipher.init(Cipher.DECRYPT_MODE, key, gcmSpec);
            final byte[] plainText = cipher.doFinal(cipherText);
            
            return new String(plainText, StandardCharsets.UTF_8);
            
        } catch (Exception e) {
            throw new SecurityException("Decryption failed: " + e.getMessage(), e);
        }
    }

    /**
     * Encrypts a string using AES-256-GCM.
     *
     * @param plainText Text to encrypt
     * @param secretKey Base64 encoded secret key
     * @return EncryptedObject containing original text and encrypted data
     * @throws Exception if encryption fails
     */
    public static EncryptedObject encrypt(String plainText, String secretKey) throws Exception {
        try {
            // Generate cryptographically secure random values
            final byte[] iv = generateSecureRandomBytes(GCM_IV_LENGTH);
            final byte[] salt = generateSecureRandomBytes(SALT_LENGTH);
            
            // Encrypt the plaintext
            final byte[] encryptedBytes = encryptInternal(plainText, secretKey, salt, iv);
            
            // Combine IV + salt + ciphertext
            final ByteBuffer buffer = ByteBuffer.allocate(iv.length + salt.length + encryptedBytes.length);
            buffer.put(iv).put(salt).put(encryptedBytes);
            
            final String encryptedWithMetadata = Base64.getEncoder().encodeToString(buffer.array());
            return new EncryptedObject(plainText, encryptedWithMetadata);
            
        } catch (Exception e) {
            throw new SecurityException("Encryption failed: " + e.getMessage(), e);
        }
    }

    /**
     * Internal encryption method with explicit parameters.
     */
    private static byte[] encryptInternal(String plainText, String secretKey, 
                                         byte[] salt, byte[] iv) throws Exception {
        final SecretKey key = deriveKey(secretKey, salt);
        final Cipher cipher = Cipher.getInstance(ALGORITHM);
        final GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        
        cipher.init(Cipher.ENCRYPT_MODE, key, gcmSpec);
        return cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Derives a secret key using PBKDF2 with high iteration count.
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
     * Generates cryptographically secure random bytes.
     */
    private static byte[] generateSecureRandomBytes(int length) {
        final byte[] bytes = new byte[length];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * Utility method to generate a new secure random key.
     */
    public static String generateSecureKey() {
        byte[] keyBytes = generateSecureRandomBytes(32); // 256 bits
        return Base64.getEncoder().encodeToString(keyBytes);
    }

    /**
     * Validates if a string appears to be properly encrypted by this class.
     */
    public static boolean isValidEncryptedFormat(String encryptedText) {
        if (encryptedText == null || encryptedText.isBlank()) {
            return false;
        }
        
        try {
            byte[] decoded = Base64.getDecoder().decode(encryptedText);
            return decoded.length >= (GCM_IV_LENGTH + SALT_LENGTH + 1); // Minimum viable encrypted data
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Benchmark and test method.
     */
    public static void main(String[] args) throws Exception {
        // Warm up the JVM and load required classes
        encrypt("warmup", "S3VtYXJhcGFuZGlhbg==");
        
        final String stringToEncrypt = "Hello There";
        final String key = "S3VtYXJhcGFuZGlhbg==";

        System.out.println("Starting encryption/decryption benchmark...");
        
        final long startTime = System.currentTimeMillis();
        final EncryptedObject encrypted = encrypt(stringToEncrypt, key);
        final long encryptionTime = System.currentTimeMillis();
        
        final String decrypted = decrypt(encrypted.getEncryptedWithIvAndSalt(), key);
        final long decryptionTime = System.currentTimeMillis();

        // Verify the result
        final boolean success = stringToEncrypt.equals(decrypted);
        
        System.out.println("Original: '%s'".formatted(stringToEncrypt));
        System.out.println("Encrypted: %s...".formatted(
            encrypted.getEncryptedWithIvAndSalt().substring(0, Math.min(50, encrypted.getEncryptedWithIvAndSalt().length()))));
        System.out.println("Decrypted: '%s'".formatted(decrypted));
        System.out.println("Success: " + success);
        System.out.println("Encryption time: %d ms".formatted(encryptionTime - startTime));
        System.out.println("Decryption time: %d ms".formatted(decryptionTime - encryptionTime));
        System.out.println("Total time: %d ms".formatted(decryptionTime - startTime));
        
        // Test key generation
        String newKey = generateSecureKey();
        System.out.println("Generated key: %s".formatted(newKey));
        System.out.println("Is valid format: %s".formatted(isValidEncryptedFormat(encrypted.getEncryptedWithIvAndSalt())));
    }
}