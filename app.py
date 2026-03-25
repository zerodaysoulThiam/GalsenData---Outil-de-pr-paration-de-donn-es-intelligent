"""
DataClean — Flask + SQLAlchemy (SQLite dev / MySQL prod)
Nettoyage intelligent de données CSV avec gestion utilisateurs
"""

import os
import uuid
import json
import bcrypt
import jwt
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template, request, jsonify, send_file, abort, g
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
from io import StringIO, BytesIO
from flask_cors import CORS

from dotenv import load_dotenv
load_dotenv()

from database import db, User, Session as DBSession, ProcessingJob, AuthSession, PasswordResetToken

# ─── App ───
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dataclean-secret-2024')
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'dataclean-jwt-secret-2024')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['MAX_CONTENT_LENGTH'] = int(os.environ.get('MAX_CONTENT_LENGTH', 50 * 1024 * 1024))
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')

# ─── DB : SQLite (défaut) ou MySQL (DB_ENGINE=mysql) ───
_db_engine = os.environ.get('DB_ENGINE', 'sqlite').lower()
if _db_engine == 'mysql':
    app.config['SQLALCHEMY_DATABASE_URI'] = (
        f"mysql+pymysql://{os.environ.get('DB_USER', 'root')}:"
        f"{os.environ.get('DB_PASSWORD', '')}@"
        f"{os.environ.get('DB_HOST', 'localhost')}:"
        f"{os.environ.get('DB_PORT', '3306')}/"
        f"{os.environ.get('DB_NAME', 'dataclean')}?charset=utf8mb4"
    )
else:
    _sqlite_dir = os.path.join(os.path.dirname(__file__), 'instance')
    os.makedirs(_sqlite_dir, exist_ok=True)
    app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{os.path.join(_sqlite_dir, 'dataclean.db')}"

ALLOWED_EXTENSIONS = {'csv', 'tsv', 'txt'}
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

db.init_app(app)

CORS(app, origins=['http://localhost:5000', 'http://127.0.0.1:5000'], supports_credentials=True)

# ─── Créer les tables au démarrage ───
with app.app_context():
    db.create_all()


# ─── Décorateurs auth ───

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
        if not token:
            return jsonify({'error': 'Token manquant', 'auth_required': True}), 401
        try:
            data = jwt.decode(token, app.config['JWT_SECRET_KEY'], algorithms=['HS256'])
            current_user = User.query.get(data['user_id'])
            if not current_user or not current_user.is_active:
                return jsonify({'error': 'Utilisateur invalide ou désactivé'}), 401
            auth_session = AuthSession.query.filter_by(token=token, user_id=current_user.id).first()
            if not auth_session or auth_session.expires_at < datetime.utcnow():
                return jsonify({'error': 'Session expirée', 'auth_required': True}), 401
            g.current_user = current_user
            g.current_token = token
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expiré', 'auth_required': True}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Token invalide', 'auth_required': True}), 401
        except Exception:
            return jsonify({'error': "Erreur d'authentification"}), 401
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not hasattr(g, 'current_user') or g.current_user.role != 'admin':
            return jsonify({'error': 'Accès admin requis'}), 403
        return f(*args, **kwargs)
    return decorated


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ─── Routes Auth ───

@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.get_json()
    for field in ['username', 'email', 'password']:
        if not data.get(field):
            return jsonify({'error': f'Le champ {field} est requis'}), 400
    if User.query.filter_by(username=data['username']).first():
        return jsonify({'error': "Ce nom d'utilisateur existe déjà"}), 400
    if User.query.filter_by(email=data['email']).first():
        return jsonify({'error': 'Cette adresse email est déjà utilisée'}), 400
    if len(data['password']) < 6:
        return jsonify({'error': 'Le mot de passe doit contenir au moins 6 caractères'}), 400
    user = User(
        id=str(uuid.uuid4()),
        username=data['username'],
        email=data['email'],
        first_name=data.get('first_name', ''),
        last_name=data.get('last_name', ''),
        role='user',
        is_active=True
    )
    user.set_password(data['password'])
    try:
        db.session.add(user)
        db.session.commit()
        return jsonify({
            'message': 'Inscription réussie',
            'user': {'id': user.id, 'username': user.username, 'email': user.email, 'role': user.role}
        }), 201
    except Exception:
        db.session.rollback()
        return jsonify({'error': "Erreur lors de l'inscription"}), 500


@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({'error': "Nom d'utilisateur et mot de passe requis"}), 400
    user = User.query.filter((User.username == username) | (User.email == username)).first()
    if not user or not user.check_password(password):
        return jsonify({'error': 'Identifiants invalides'}), 401
    if not user.is_active:
        return jsonify({'error': 'Compte désactivé'}), 403
    expires_hours = int(os.environ.get('JWT_ACCESS_TOKEN_EXPIRES', 24))
    token = jwt.encode({
        'user_id': user.id,
        'username': user.username,
        'role': user.role,
        'exp': datetime.utcnow() + timedelta(hours=expires_hours)
    }, app.config['JWT_SECRET_KEY'], algorithm='HS256')
    auth_session = AuthSession(
        id=str(uuid.uuid4()),
        user_id=user.id,
        token=token,
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent'),
        expires_at=datetime.utcnow() + timedelta(hours=expires_hours)
    )
    user.last_login = datetime.utcnow()
    try:
        db.session.add(auth_session)
        db.session.commit()
        return jsonify({
            'message': 'Connexion réussie',
            'token': token,
            'user': {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'role': user.role
            }
        })
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Erreur lors de la connexion'}), 500


@app.route('/api/auth/logout', methods=['POST'])
@token_required
def logout():
    try:
        auth_session = AuthSession.query.filter_by(token=g.current_token).first()
        if auth_session:
            db.session.delete(auth_session)
            db.session.commit()
        return jsonify({'message': 'Déconnexion réussie'})
    except Exception:
        return jsonify({'error': 'Erreur lors de la déconnexion'}), 500


@app.route('/api/auth/me', methods=['GET'])
@token_required
def get_current_user():
    return jsonify({
        'user': {
            'id': g.current_user.id,
            'username': g.current_user.username,
            'email': g.current_user.email,
            'first_name': g.current_user.first_name,
            'last_name': g.current_user.last_name,
            'role': g.current_user.role,
            'created_at': g.current_user.created_at.isoformat() if g.current_user.created_at else None
        }
    })


@app.route('/api/auth/change-password', methods=['POST'])
@token_required
def change_password():
    data = request.get_json()
    old_password = data.get('old_password')
    new_password = data.get('new_password')
    if not old_password or not new_password:
        return jsonify({'error': 'Ancien et nouveau mot de passe requis'}), 400
    if len(new_password) < 6:
        return jsonify({'error': 'Le nouveau mot de passe doit contenir au moins 6 caractères'}), 400
    if not g.current_user.check_password(old_password):
        return jsonify({'error': 'Ancien mot de passe incorrect'}), 401
    g.current_user.set_password(new_password)
    try:
        db.session.commit()
        return jsonify({'message': 'Mot de passe changé avec succès'})
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Erreur lors du changement de mot de passe'}), 500


@app.route('/api/auth/reset-password-request', methods=['POST'])
def reset_password_request():
    data = request.get_json()
    email = data.get('email')
    if not email:
        return jsonify({'error': 'Email requis'}), 400
    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({'message': "Si un compte existe avec cet email, un lien de réinitialisation a été envoyé"})
    token = str(uuid.uuid4())
    reset_token = PasswordResetToken(
        user_id=user.id,
        token=token,
        expires_at=datetime.utcnow() + timedelta(hours=24)
    )
    try:
        db.session.add(reset_token)
        db.session.commit()
        return jsonify({
            'message': "Si un compte existe avec cet email, un lien de réinitialisation a été envoyé",
            'reset_token': token  # Supprimer en production
        })
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Erreur lors de la demande'}), 500


@app.route('/api/auth/reset-password', methods=['POST'])
def reset_password():
    data = request.get_json()
    token = data.get('token')
    new_password = data.get('new_password')
    if not token or not new_password:
        return jsonify({'error': 'Token et nouveau mot de passe requis'}), 400
    if len(new_password) < 6:
        return jsonify({'error': 'Le mot de passe doit contenir au moins 6 caractères'}), 400
    reset_token = PasswordResetToken.query.filter_by(token=token, used=False).first()
    if not reset_token or reset_token.expires_at < datetime.utcnow():
        return jsonify({'error': 'Token invalide ou expiré'}), 400
    user = User.query.get(reset_token.user_id)
    if not user:
        return jsonify({'error': 'Utilisateur non trouvé'}), 404
    user.set_password(new_password)
    reset_token.used = True
    try:
        db.session.commit()
        AuthSession.query.filter_by(user_id=user.id).delete()
        db.session.commit()
        return jsonify({'message': 'Mot de passe réinitialisé avec succès'})
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Erreur lors de la réinitialisation'}), 500


# ─── Routes Admin ───

@app.route('/api/admin/users', methods=['GET'])
@token_required
@admin_required
def list_users():
    users = User.query.all()
    return jsonify({
        'users': [{
            'id': u.id,
            'username': u.username,
            'email': u.email,
            'first_name': u.first_name,
            'last_name': u.last_name,
            'role': u.role,
            'is_active': u.is_active,
            'last_login': u.last_login.isoformat() if u.last_login else None,
            'created_at': u.created_at.isoformat() if u.created_at else None,
            'sessions_count': u.sessions.count()
        } for u in users]
    })


@app.route('/api/admin/users/<user_id>', methods=['PUT'])
@token_required
@admin_required
def update_user(user_id):
    data = request.get_json()
    user = User.query.get_or_404(user_id)
    for field in ['role', 'is_active', 'first_name', 'last_name']:
        if field in data:
            setattr(user, field, data[field])
    try:
        db.session.commit()
        return jsonify({'message': 'Utilisateur mis à jour'})
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Erreur lors de la mise à jour'}), 500


@app.route('/api/admin/users/<user_id>', methods=['DELETE'])
@token_required
@admin_required
def delete_user(user_id):
    if user_id == g.current_user.id:
        return jsonify({'error': 'Vous ne pouvez pas supprimer votre propre compte'}), 400
    user = User.query.get_or_404(user_id)
    try:
        db.session.delete(user)
        db.session.commit()
        return jsonify({'message': 'Utilisateur supprimé'})
    except Exception:
        db.session.rollback()
        return jsonify({'error': 'Erreur lors de la suppression'}), 500


# ─── Routes Data ───

@app.route('/api/upload', methods=['POST'])
@token_required
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier fourni'}), 400
    file = request.files['file']
    if not file or file.filename == '':
        return jsonify({'error': 'Fichier vide'}), 400
    if not allowed_file(file.filename):
        return jsonify({'error': 'Format non supporté. Utilisez CSV, TSV ou TXT'}), 400
    try:
        session_id = str(uuid.uuid4())
        original_name = secure_filename(file.filename)
        user_folder = os.path.join(app.config['UPLOAD_FOLDER'], g.current_user.id)
        os.makedirs(user_folder, exist_ok=True)
        save_path = os.path.join(user_folder, f"{session_id}_{original_name}")
        file.save(save_path)

        sep = '\t' if original_name.endswith('.tsv') else ','
        try:
            df = pd.read_csv(save_path, sep=sep, low_memory=False)
        except Exception:
            df = pd.read_csv(save_path, sep=sep, low_memory=False, encoding='latin-1')

        if df.empty or len(df.columns) == 0:
            os.remove(save_path)
            return jsonify({'error': 'Fichier vide ou non parseable'}), 400

        analysis = analyze_dataframe(df)

        session = DBSession(
            id=session_id,
            user_id=g.current_user.id,
            original_filename=original_name,
            file_path=save_path,
            rows=len(df),
            columns=len(df.columns),
            column_names=json.dumps(list(df.columns)),
            analysis=json.dumps(analysis),
        )
        db.session.add(session)
        db.session.commit()

        return jsonify({
            'session_id': session_id,
            'filename': original_name,
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'preview': df.head(10).fillna('').astype(str).to_dict(orient='records'),
            'analysis': analysis,
        })
    except Exception as e:
        return jsonify({'error': f'Erreur lors de la lecture: {str(e)}'}), 500


@app.route('/api/process', methods=['POST'])
@token_required
def process():
    data = request.get_json()
    session_id = data.get('session_id')
    options = data.get('options', {})
    if not session_id:
        return jsonify({'error': 'session_id requis'}), 400

    session = DBSession.query.filter_by(id=session_id, user_id=g.current_user.id).first()
    if not session:
        return jsonify({'error': 'Session introuvable'}), 404

    try:
        sep = '\t' if session.original_filename.endswith('.tsv') else ','
        try:
            df = pd.read_csv(session.file_path, sep=sep, low_memory=False)
        except Exception:
            df = pd.read_csv(session.file_path, sep=sep, low_memory=False, encoding='latin-1')

        from processor import process_dataframe
        processed_df, report = process_dataframe(df, options)

        job_id = str(uuid.uuid4())
        user_folder = os.path.join(app.config['UPLOAD_FOLDER'], g.current_user.id)
        result_path = os.path.join(user_folder, f"{session_id}_result_{job_id}.csv")
        processed_df.to_csv(result_path, index=False)

        job = ProcessingJob(
            id=job_id,
            session_id=session_id,
            options=json.dumps(options),
            report=json.dumps(report),
            result_path=result_path,
            final_rows=len(processed_df),
            final_columns=len(processed_df.columns),
        )
        db.session.add(job)
        db.session.commit()

        return jsonify({
            'job_id': job_id,
            'report': report,
            'preview': processed_df.head(20).fillna('').astype(str).to_dict(orient='records'),
            'columns': list(processed_df.columns),
            'final_rows': len(processed_df),
            'final_columns': len(processed_df.columns),
        })
    except Exception as e:
        return jsonify({'error': f'Erreur de traitement: {str(e)}'}), 500


@app.route('/api/download/<job_id>')
@token_required
def download(job_id):
    """Téléchargement sécurisé via Bearer token (fetch + blob côté client)"""
    job = ProcessingJob.query.get(job_id)
    if not job:
        abort(404)
    session = DBSession.query.get(job.session_id)
    if not session:
        abort(404)
    if session.user_id != g.current_user.id and g.current_user.role != 'admin':
        abort(403)
    if not os.path.exists(job.result_path):
        abort(404)

    download_name = session.original_filename.rsplit('.', 1)[0] + '_nettoye.csv'
    return send_file(
        job.result_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=download_name
    )


@app.route('/api/history')
@token_required
def history():
    sessions = DBSession.query.filter_by(user_id=g.current_user.id)\
        .order_by(DBSession.created_at.desc()).limit(20).all()
    result = []
    for s in sessions:
        jobs = ProcessingJob.query.filter_by(session_id=s.id).all()
        result.append({
            'id': s.id,
            'filename': s.original_filename,
            'rows': s.rows,
            'columns': s.columns,
            'created_at': s.created_at.isoformat(),
            'jobs_count': len(jobs),
        })
    return jsonify(result)


@app.route('/api/session/<session_id>/analysis')
@token_required
def session_analysis(session_id):
    session = DBSession.query.filter_by(id=session_id, user_id=g.current_user.id).first_or_404()
    return jsonify({
        'id': session.id,
        'filename': session.original_filename,
        'rows': session.rows,
        'columns': session.columns,
        'column_names': json.loads(session.column_names) if session.column_names else [],
        'analysis': json.loads(session.analysis) if session.analysis else {},
        'created_at': session.created_at.isoformat(),
    })


@app.route('/api/stats')
@token_required
def global_stats():
    total_sessions = DBSession.query.filter_by(user_id=g.current_user.id).count()
    total_jobs = ProcessingJob.query.join(DBSession)\
        .filter(DBSession.user_id == g.current_user.id).count()
    total_rows = db.session.query(db.func.sum(DBSession.rows))\
        .filter(DBSession.user_id == g.current_user.id).scalar() or 0
    return jsonify({
        'total_sessions': total_sessions,
        'total_jobs': total_jobs,
        'total_rows_processed': int(total_rows),
    })


# ─── Analyse helper ───

def analyze_dataframe(df: pd.DataFrame) -> dict:
    analysis = {
        'missing_total': int(df.isnull().sum().sum()),
        'duplicate_rows': int(df.duplicated().sum()),
        'columns': []
    }
    for col in df.columns:
        col_data = df[col]
        missing = int(col_data.isnull().sum())
        col_type = 'numeric' if pd.api.types.is_numeric_dtype(col_data) else 'text'
        col_info = {
            'name': col,
            'type': col_type,
            'missing': missing,
            'missing_pct': round(missing / len(df) * 100, 1) if len(df) > 0 else 0,
            'unique': int(col_data.nunique()),
        }
        if col_type == 'numeric':
            nums = col_data.dropna()
            if len(nums) > 3:
                q1 = float(nums.quantile(0.25))
                q3 = float(nums.quantile(0.75))
                iqr = q3 - q1
                outliers = int(((nums < q1 - 1.5 * iqr) | (nums > q3 + 1.5 * iqr)).sum())
                col_info.update({
                    'min': round(float(nums.min()), 4),
                    'max': round(float(nums.max()), 4),
                    'mean': round(float(nums.mean()), 4),
                    'outliers_iqr': outliers,
                })
        analysis['columns'].append(col_info)
    return analysis


# ─── Pages ───

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login.html')
def login_page():
    return render_template('login.html')


if __name__ == '__main__':
    app.run(debug=True, port=5000)
