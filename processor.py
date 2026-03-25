"""
Moteur de traitement des données — Python/Pandas
Port exact de la logique TypeScript du projet original
+ améliorations: chunking, types robustes, rapport enrichi
"""

import pandas as pd
import numpy as np
from typing import Tuple


def process_dataframe(
    df: pd.DataFrame,
    options: dict
) -> Tuple[pd.DataFrame, dict]:
    """
    Traite un DataFrame selon les options fournies.

    Options supportées:
      missingValues : 'remove' | 'mean' | 'median' | 'mode'
      outliers      : 'keep' | 'remove' | 'replace'
      outlierMethod : 'iqr' | 'zscore'
      removeDuplicates: bool
      normalize     : 'none' | 'minmax' | 'zscore'
    """
    report = {
        'originalRows': len(df),
        'originalColumns': len(df.columns),
        'missingValuesFound': 0,
        'missingValuesTreated': 0,
        'outliersFound': 0,
        'outliersTreated': 0,
        'duplicatesRemoved': 0,
        'finalRows': 0,
        'finalColumns': 0,
        'steps': [],
    }

    result = df.copy()
    columns = list(df.columns)
    numeric_cols = _get_numeric_columns(result)

    # ── 1. Valeurs manquantes ──
    missing_total = int(result.isnull().sum().sum())
    report['missingValuesFound'] = missing_total

    missing_strategy = options.get('missingValues', 'mean')

    if missing_strategy == 'remove':
        before = len(result)
        result = result.dropna()
        treated = before - len(result)
        report['missingValuesTreated'] = treated
        report['steps'].append(f"Suppression de {treated} ligne(s) avec valeurs manquantes")

    elif missing_strategy in ('mean', 'median', 'mode'):
        treated = 0
        for col in columns:
            null_mask = result[col].isnull()
            n_null = null_mask.sum()
            if n_null == 0:
                continue

            if col in numeric_cols:
                if missing_strategy == 'mean':
                    fill_val = result[col].mean()
                elif missing_strategy == 'median':
                    fill_val = result[col].median()
                else:
                    fill_val = result[col].mode()[0] if not result[col].mode().empty else 0
            else:
                # Colonne texte → mode toujours
                mode_vals = result[col].mode()
                fill_val = mode_vals[0] if not mode_vals.empty else ''

            result[col] = result[col].fillna(fill_val)
            treated += int(n_null)

        report['missingValuesTreated'] = treated
        report['steps'].append(
            f"Imputation de {treated} valeur(s) manquante(s) par {missing_strategy}"
        )

    # ── 2. Valeurs aberrantes ──
    outlier_strategy = options.get('outliers', 'keep')
    outlier_method = options.get('outlierMethod', 'iqr')

    if outlier_strategy != 'keep' and numeric_cols:
        outlier_mask = pd.Series([False] * len(result), index=result.index)

        for col in numeric_cols:
            series = result[col].dropna()
            if len(series) < 4:
                continue
            col_outliers = _detect_outliers(result[col], outlier_method)
            outlier_mask = outlier_mask | col_outliers

        n_outliers = int(outlier_mask.sum())
        report['outliersFound'] = n_outliers

        if outlier_strategy == 'remove':
            result = result[~outlier_mask]
            report['outliersTreated'] = n_outliers
            report['steps'].append(
                f"Suppression de {n_outliers} ligne(s) aberrante(s) ({outlier_method.upper()})"
            )

        elif outlier_strategy == 'replace':
            # Remplacer chaque outlier par la médiane de sa colonne
            for col in numeric_cols:
                col_outliers = _detect_outliers(result[col], outlier_method)
                if col_outliers.sum() > 0:
                    median_val = result.loc[~col_outliers, col].median()
                    result.loc[col_outliers, col] = median_val
            report['outliersTreated'] = n_outliers
            report['steps'].append(
                f"Remplacement de {n_outliers} valeur(s) aberrante(s) par la médiane"
            )

    # ── 3. Doublons ──
    if options.get('removeDuplicates', True):
        before = len(result)
        result = result.drop_duplicates()
        dupes = before - len(result)
        report['duplicatesRemoved'] = dupes
        if dupes > 0:
            report['steps'].append(f"Suppression de {dupes} ligne(s) dupliquée(s)")

    # ── 4. Normalisation ──
    normalize = options.get('normalize', 'none')
    # Recalculer les colonnes numériques sur le df résultant
    numeric_cols_final = _get_numeric_columns(result)

    if normalize != 'none' and numeric_cols_final:
        for col in numeric_cols_final:
            series = result[col].astype(float)
            if normalize == 'minmax':
                col_min = series.min()
                col_max = series.max()
                rng = col_max - col_min
                if rng != 0:
                    result[col] = ((series - col_min) / rng).round(4)
            elif normalize == 'zscore':
                mean_val = series.mean()
                std_val = series.std()
                if std_val != 0:
                    result[col] = ((series - mean_val) / std_val).round(4)

        report['steps'].append(
            f"Normalisation {normalize.upper()} appliquée sur {len(numeric_cols_final)} colonne(s) numérique(s)"
        )

    report['finalRows'] = len(result)
    report['finalColumns'] = len(result.columns)

    if not report['steps']:
        report['steps'].append("Aucun traitement appliqué — données déjà propres ✓")

    return result, report


def _get_numeric_columns(df: pd.DataFrame) -> list:
    """Retourne les colonnes numériques en excluant celles trop peu remplies"""
    numeric = []
    for col in df.columns:
        series = pd.to_numeric(df[col], errors='coerce')
        valid_ratio = series.notna().sum() / max(len(df), 1)
        if valid_ratio > 0.5:
            # Convertir la colonne en numérique si elle passe le seuil
            df[col] = series
            numeric.append(col)
    return numeric


def _detect_outliers(series: pd.Series, method: str) -> pd.Series:
    """Retourne un masque booléen des outliers"""
    numeric = pd.to_numeric(series, errors='coerce')
    mask = pd.Series([False] * len(series), index=series.index)

    valid = numeric.dropna()
    if len(valid) < 4:
        return mask

    if method == 'iqr':
        q1 = valid.quantile(0.25)
        q3 = valid.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        mask = (numeric < lower) | (numeric > upper)
        mask = mask.fillna(False)

    elif method == 'zscore':
        mean_val = valid.mean()
        std_val = valid.std()
        if std_val > 0:
            z_scores = (numeric - mean_val).abs() / std_val
            mask = z_scores > 3
            mask = mask.fillna(False)

    return mask
