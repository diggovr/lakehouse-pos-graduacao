@echo off
setlocal EnableDelayedExpansion

REM ==================================================
REM  RESOLUCAO DE CAMINHOS
REM ==================================================
set SCRIPT_DIR=%~dp0
set PROJECT_DIR=%SCRIPT_DIR%..

REM ==================================================
REM  CONFIGURACOES
REM ==================================================
set AWS_PROFILE=pos_dados
set LOG_DIR=C:\Users\Administrator\Desktop\Lakehouse\log

echo =========================================
echo   PIPELINE LAKEHOUSE POS-GRADUACAO
echo =========================================
echo.

REM ==================================================
REM  CRIA DIRETORIO DE LOG
REM ==================================================
if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

REM ==================================================
REM  TIMESTAMP SEGURO (INDEPENDENTE DE LOCALE)
REM ==================================================
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DATETIME=%%I

set DATA=%DATETIME:~0,8%
set HORA=%DATETIME:~8,6%

set LOG_FILE=%LOG_DIR%\pipeline_%DATA%_%HORA%.log

echo Log de execucao: %LOG_FILE%
echo.

REM ==================================================
REM  INICIO DO LOG
REM ==================================================
echo INICIO PIPELINE > "%LOG_FILE%"
echo Data/Hora: %DATA% %HORA% >> "%LOG_FILE%"
echo Projeto: %PROJECT_DIR% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

REM ==================================================
REM  AWS PROFILE
REM ==================================================
echo AWS PROFILE ATIVO:
aws sts get-caller-identity >> "%LOG_FILE%" 2>&1
aws sts get-caller-identity
echo. >> "%LOG_FILE%"

REM ==================================================
REM  ETAPA 1 - LEGADO UPLOAD
REM ==================================================
echo ETAPA 1 - LEGADO UPLOAD
echo ETAPA 1 - LEGADO UPLOAD >> "%LOG_FILE%"
python "%PROJECT_DIR%\ETL\legado\legado_upload.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 goto erro
echo. >> "%LOG_FILE%"

REM ==================================================
REM  ETAPA 2 - LEGADO TO RAW
REM ==================================================
echo ETAPA 2 - LEGADO TO RAW
echo ETAPA 2 - LEGADO TO RAW >> "%LOG_FILE%"
python "%PROJECT_DIR%\ETL\raw\legado_to_raw.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 goto erro
echo. >> "%LOG_FILE%"

REM ==================================================
REM  ETAPA 3 - RAW TO TRUSTED
REM ==================================================
echo ETAPA 3 - RAW TO TRUSTED
echo ETAPA 3 - RAW TO TRUSTED >> "%LOG_FILE%"
python "%PROJECT_DIR%\ETL\trusted\raw_to_trusted.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 goto erro
echo. >> "%LOG_FILE%"

REM ==================================================
REM  ETAPA 4 - CTAS ATHENA (CURATED)
REM ==================================================
echo ETAPA 4 - CTAS ATHENA
echo ETAPA 4 - CTAS ATHENA >> "%LOG_FILE%"
python "%PROJECT_DIR%\ETL\curated\run_ctas_athena.py" >> "%LOG_FILE%" 2>&1
if errorlevel 1 goto erro
echo. >> "%LOG_FILE%"

REM ==================================================
REM  FINAL COM SUCESSO
REM ==================================================
echo PIPELINE FINALIZADO COM SUCESSO
echo PIPELINE FINALIZADO COM SUCESSO >> "%LOG_FILE%"
echo FIM >> "%LOG_FILE%"

pause
exit /b 0

REM ==================================================
REM  TRATAMENTO DE ERRO
REM ==================================================
:erro
echo ERRO NA EXECUCAO DO PIPELINE
echo ERRO NA EXECUCAO DO PIPELINE >> "%LOG_FILE%"
echo FIM COM ERRO >> "%LOG_FILE%"
pause
exit /b 1
