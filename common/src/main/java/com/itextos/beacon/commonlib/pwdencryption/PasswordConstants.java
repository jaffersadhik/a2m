package com.itextos.beacon.commonlib.pwdencryption;

import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * Cryptographic constants and configuration for password encryption.
 * Uses modern security standards and best practices.
 */
public final class PasswordConstants {
    
    // Private constructor to prevent instantiation
    private PasswordConstants() {
        throw new AssertionError("Cannot instantiate utility class");
    }

    // Modern cryptographic parameters for AES-GCM
    public static final int GCM_IV_LENGTH = 12;        // Recommended IV size for GCM
    public static final int GCM_TAG_LENGTH = 128;      // Authentication tag length in bits
    public static final int SALT_LENGTH = 16;          // 128-bit salt for PBKDF2
    public static final int KEY_LENGTH = 256;          // AES-256 key size in bits
    
    // Algorithm specifications
    public static final String ALGORITHM = "AES/GCM/NoPadding";
    public static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
    
    // Password-based key derivation parameters (OWASP 2024 recommendations)
    public static final int PBKDF2_ITERATIONS = 210_000;
    public static final int PBKDF2_SALT_LENGTH = 16;
    
    // Password length requirements
    public static final int GUI_PASSWORD_LENGTH = 12;
    public static final int API_PASSWORD_LENGTH = 12;
    public static final int SMPP_PASSWORD_LENGTH = 12; // Increased from 8 for better security
    public static final int MINIMUM_PASSWORD_LENGTH = 8;
    
    // Encryption keys (consider moving to secure storage in production)
    public static final String API_PASSWORD_KEY = "d2lubm92YXR1cmVhcGl0cnVzdGZvcmV2ZXI=";
    public static final String SMPP_PASSWORD_KEY = "c21wcHRydXN0d2lubm92YXR1cmVmb3JldmVy";
    public static final String DB_PASSWORD_KEY = "S3VtYXJhcGFuZGlhbmlUZXh0b3NTdW1hdGhp";
    
    // Character sets for password generation
    private static final String UPPERCASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String LOWERCASE = UPPERCASE.toLowerCase();
    private static final String NUMBERS = "0123456789";
    private static final String SPECIAL_CHARS = "!@#$%^&*()_+-=[]{}|;:,.<>?";
    
    // Combined character sets
    public static final String ALPHABETS = UPPERCASE + LOWERCASE;
    public static final String ALPHANUMERIC = ALPHABETS + NUMBERS;
    public static final String ALL_CHARS_STRING = ALPHANUMERIC + SPECIAL_CHARS;
    public static final char[] ALL_CHARS = ALL_CHARS_STRING.toCharArray();
    
    // Secure character sets for specific requirements
    public static final char[] ALPHANUMERIC_CHARS = ALPHANUMERIC.toCharArray();
    public static final char[] LETTERS_ONLY = ALPHABETS.toCharArray();
    
    // Security configuration
    public static final int MAX_PASSWORD_ATTEMPTS = 5;
    public static final int PASSWORD_VALIDITY_DAYS = 90;
    public static final int PASSWORD_HISTORY_COUNT = 5;
    
    // Key derivation parameters for different security levels
    public static enum SecurityLevel {
        STANDARD(100_000, 128),
        HIGH(210_000, 192),
        CRITICAL(600_000, 256);
        
        private final int iterations;
        private final int keyLength;
        
        SecurityLevel(int iterations, int keyLength) {
            this.iterations = iterations;
            this.keyLength = keyLength;
        }
        
        public int getIterations() { return iterations; }
        public int getKeyLength() { return keyLength; }
    }
    
    // Password complexity requirements
    public static final class Complexity {
        public static final int MIN_UPPERCASE = 1;
        public static final int MIN_LOWERCASE = 1;
        public static final int MIN_DIGITS = 1;
        public static final int MIN_SPECIAL = 1;
        public static final int MAX_CONSECUTIVE_IDENTICAL = 2;
        public static final int MAX_SEQUENTIAL_CHARS = 3;
    }
    
    // Utility methods
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final HexFormat HEX_FORMAT = HexFormat.of();
    
    /**
     * Generates a cryptographically secure random salt.
     */
    public static byte[] generateSalt() {
        byte[] salt = new byte[SALT_LENGTH];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }
    
    /**
     * Generates a secure random initialization vector for GCM.
     */
    public static byte[] generateIv() {
        byte[] iv = new byte[GCM_IV_LENGTH];
        SECURE_RANDOM.nextBytes(iv);
        return iv;
    }
    
    /**
     * Converts bytes to hex string for debugging and logging.
     */
    public static String toHexString(byte[] bytes) {
        return HEX_FORMAT.formatHex(bytes);
    }
    
    /**
     * Validates if a password meets basic complexity requirements.
     */
    public static boolean meetsComplexityRequirements(String password) {
        if (password == null || password.length() < MINIMUM_PASSWORD_LENGTH) {
            return false;
        }
        
        boolean hasUpper = password.chars().anyMatch(Character::isUpperCase);
        boolean hasLower = password.chars().anyMatch(Character::isLowerCase);
        boolean hasDigit = password.chars().anyMatch(Character::isDigit);
        boolean hasSpecial = password.chars().anyMatch(ch -> 
            SPECIAL_CHARS.indexOf(ch) >= 0);
        
        return hasUpper && hasLower && hasDigit && hasSpecial;
    }
    
    /**
     * Gets the recommended security level based on application context.
     */
    public static SecurityLevel getRecommendedSecurityLevel(String context) {
        return switch (context.toLowerCase()) {
            case "database", "api" -> SecurityLevel.HIGH;
            case "gui", "user" -> SecurityLevel.STANDARD;
            case "master", "root" -> SecurityLevel.CRITICAL;
            default -> SecurityLevel.STANDARD;
        };
    }
    
    // Deprecation notice for old constants (maintains backward compatibility)
    /**
     * @deprecated Use {@link #GCM_IV_LENGTH} instead for GCM mode
     */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final int INITIAL_VECTOR_LENGTH = 16;
    
    /**
     * @deprecated Use {@link #SALT_LENGTH} instead
     */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final int SALT_LEGNTH = 10;
    
    /**
     * @deprecated Use {@link #ALGORITHM} with GCM mode instead
     */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final String CBC_ALGORITHM = "AES/CBC/PKCS5Padding";
}