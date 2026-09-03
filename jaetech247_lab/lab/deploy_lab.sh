#!/bin/bash
# deploy_lab.sh — jaetech247_lab 배포 스크립트
set -e

LAB_DIR="/home/ubuntu/jaetech247_lab"
MAIN_VENV="/home/ubuntu/jaetech247/venv"
SERVICE="jaetech247_lab"

echo "=== JaeTech247 Lab 배포 ==="

# 1. 디렉토리 생성
mkdir -p "$LAB_DIR/lab/templates"
mkdir -p "$LAB_DIR/trading"

# 2. 파일 복사
cp -r lab/ "$LAB_DIR/"
cp trading/futures_runner.py "$LAB_DIR/trading/"
touch "$LAB_DIR/trading/__init__.py"

# 3. 메인 앱의 venv 재사용 (requests, fastapi 이미 설치됨)
# 추가 패키지 없음

# 4. systemd 서비스 등록
sudo cp jaetech247_lab.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE"
sudo systemctl restart "$SERVICE"

echo ""
echo "✓ Lab 서비스 시작됨 (포트 8081)"
echo "  상태 확인: sudo systemctl status $SERVICE"
echo "  로그 확인: sudo journalctl -u $SERVICE -f"
echo ""
echo "=== nginx /lab 경로 설정 ==="
echo "아래 블록을 /etc/nginx/sites-available/jaetech247.pro 의"
echo "'location /' 블록 위에 추가하세요:"
echo ""
cat << 'NGINX'
    # Lab — 바이낸스 선물 그리드 (포트 8081)
    location /lab {
        proxy_pass             http://127.0.0.1:8081;
        proxy_http_version     1.1;
        proxy_set_header       Host $host;
        proxy_set_header       X-Real-IP $remote_addr;
        proxy_set_header       X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header       X-Forwarded-Proto $scheme;
        proxy_read_timeout     60s;
    }
    location /lab/ws {
        proxy_pass             http://127.0.0.1:8081;
        proxy_http_version     1.1;
        proxy_set_header       Upgrade $http_upgrade;
        proxy_set_header       Connection "upgrade";
        proxy_set_header       Host $host;
        proxy_read_timeout     3600s;
    }
NGINX
echo ""
echo "추가 후: sudo nginx -t && sudo systemctl reload nginx"
