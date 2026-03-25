# database.py
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
import bcrypt

db = SQLAlchemy()


class User(db.Model):
    __tablename__ = 'users'

    id = db.Column(db.String(36), primary_key=True)
    username = db.Column(db.String(50), nullable=False, unique=True)
    email = db.Column(db.String(100), nullable=False, unique=True)
    password_hash = db.Column(db.String(255), nullable=False)
    first_name = db.Column(db.String(50))
    last_name = db.Column(db.String(50))
    role = db.Column(db.Enum('admin', 'user'), default='user')
    is_active = db.Column(db.Boolean, default=True)
    last_login = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sessions = db.relationship('Session', backref='user', lazy='dynamic', cascade='all, delete-orphan')
    auth_sessions = db.relationship('AuthSession', backref='user', lazy=True, cascade='all, delete-orphan')
    reset_tokens = db.relationship('PasswordResetToken', backref='user', lazy=True, cascade='all, delete-orphan')

    def set_password(self, password: str):
        salt = bcrypt.gensalt()
        self.password_hash = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    def check_password(self, password: str) -> bool:
        return bcrypt.checkpw(password.encode('utf-8'), self.password_hash.encode('utf-8'))

    def __repr__(self):
        return f'<User {self.username}>'


class Session(db.Model):
    __tablename__ = 'sessions'

    id = db.Column(db.String(36), primary_key=True)
    user_id = db.Column(db.String(36), db.ForeignKey('users.id'), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    file_path = db.Column(db.String(500), nullable=False)
    # CORRECTION : nommage unifié avec app.py (rows / columns)
    rows = db.Column(db.Integer, default=0)
    columns = db.Column(db.Integer, default=0)
    column_names = db.Column(db.Text)
    analysis = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    jobs = db.relationship('ProcessingJob', backref='session', lazy=True, cascade='all, delete-orphan')

    def __repr__(self):
        return f'<Session {self.original_filename}>'


class ProcessingJob(db.Model):
    __tablename__ = 'processing_jobs'

    id = db.Column(db.String(36), primary_key=True)
    session_id = db.Column(db.String(36), db.ForeignKey('sessions.id'), nullable=False)
    options = db.Column(db.Text)
    report = db.Column(db.Text)
    result_path = db.Column(db.String(500))
    final_rows = db.Column(db.Integer, default=0)
    final_columns = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<ProcessingJob {self.id[:8]}>'


class AuthSession(db.Model):
    __tablename__ = 'auth_sessions'

    id = db.Column(db.String(36), primary_key=True)
    user_id = db.Column(db.String(36), db.ForeignKey('users.id'), nullable=False)
    token = db.Column(db.String(512), nullable=False, unique=True)
    ip_address = db.Column(db.String(45))
    user_agent = db.Column(db.Text)
    expires_at = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<AuthSession {self.user_id}>'


class PasswordResetToken(db.Model):
    __tablename__ = 'password_reset_tokens'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.String(36), db.ForeignKey('users.id'), nullable=False)
    token = db.Column(db.String(255), nullable=False, unique=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    used = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<PasswordResetToken {self.token[:8]}>'
