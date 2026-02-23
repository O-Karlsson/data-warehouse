# Dataset notes

_Generated on 2026-02-19 11:26:39_

- This dataset contains 1x1 period life tables from the Human Lifetable Database (HLD) for Italy.
- Source data are distributed via lifetable.de (Human Lifetable Database).
- Raw inputs are stored as the original ZIP file only (no extracted files are kept in the raw folder).
- Before saving, variable names are made Stata-safe by removing '(', ')', and '-' and then sanitizing other illegal characters (to ensure Stata compatibility).
- Life table columns are renamed to standard short forms: m(x)->mx, q(x)->qx, l(x)->lx, d(x)->dx, L(x)->lx_1, T(x)->tx, e(x)->ex, e(x)Orig->exOrig.
- Columns that look numeric are coerced to numeric where possible.
- The column `source_file` indicates which extracted file each row came from.
