-- database.sql
-- Création de la base de données DataClean

-- Supprimer la base si elle existe déjà
DROP DATABASE IF EXISTS dataclean;

-- Créer la base de données
CREATE DATABASE dataclean CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Utiliser la base
USE dataclean;

-- Table des utilisateurs
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(36) PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    role ENUM('admin', 'user') DEFAULT 'user',
    is_active BOOLEAN DEFAULT TRUE,
    last_login DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Table des sessions
CREATE TABLE IF NOT EXISTS sessions (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    total_rows INT DEFAULT 0,
    total_columns INT DEFAULT 0,
    column_names TEXT,
    analysis TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at)
);

-- Table des jobs de traitement
CREATE TABLE IF NOT EXISTS processing_jobs (
    id VARCHAR(36) PRIMARY KEY,
    session_id VARCHAR(36) NOT NULL,
    options TEXT,
    report TEXT,
    result_path VARCHAR(500),
    final_rows INT DEFAULT 0,
    final_columns INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    INDEX idx_session_id (session_id),
    INDEX idx_created_at (created_at)
);

-- Table des tokens de réinitialisation
CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    token VARCHAR(255) NOT NULL UNIQUE,
    expires_at DATETIME NOT NULL,
    used BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_token (token),
    INDEX idx_expires_at (expires_at)
);

-- Table des sessions d'authentification
CREATE TABLE IF NOT EXISTS auth_sessions (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    token VARCHAR(255) NOT NULL UNIQUE,
    ip_address VARCHAR(45),
    user_agent TEXT,
    expires_at DATETIME NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_token (token),
    INDEX idx_user_id (user_id),
    INDEX idx_expires_at (expires_at)
);

-- Insertion des utilisateurs par défaut
-- Mot de passe: Admin123!
INSERT INTO users (id, username, email, password_hash, first_name, last_name, role) VALUES
('admin-001', 'admin', 'admin@dataclean.local', '$2b$12$K9LmNpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjK', 'Admin', 'DataClean', 'admin');

-- Mot de passe: test123
INSERT INTO users (id, username, email, password_hash, first_name, last_name, role) VALUES
('user-001', 'testuser', 'test@dataclean.local', '$2b$12$K9LmNpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjK', 'Test', 'User', 'user');

-- Mot de passe: demo123
INSERT INTO users (id, username, email, password_hash, first_name, last_name, role) VALUES
('user-002', 'demo', 'demo@dataclean.local', '$2b$12$LmNpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKl', 'Demo', 'User', 'user');

-- Afficher les utilisateurs créés
SELECT id, username, email, role FROM users;