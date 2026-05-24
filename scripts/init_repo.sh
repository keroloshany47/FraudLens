#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# FraudLens — Repo Initialization Script
# Run this ONCE after cloning to set up your local environment
# Usage: bash scripts/init_repo.sh
# ─────────────────────────────────────────────────────────────────

set -e

CYAN='\033[36m'
GREEN='\033[32m'
YELLOW='\033[33m'
RED='\033[31m'
RESET='\033[0m'

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${CYAN} FraudLens — Repository Setup${RESET}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

# 1. Check prerequisites
echo -e "${CYAN} Checking prerequisites...${RESET}"
command -v docker >/dev/null || { echo -e "${RED} Docker not found${RESET}"; exit 1; }
command -v git >/dev/null || { echo -e "${RED} Git not found${RESET}"; exit 1; }
echo -e "${GREEN} Docker: $(docker --version | cut -d' ' -f3 | tr -d ',')${RESET}"
echo -e "${GREEN} Git: $(git --version | cut -d' ' -f3)${RESET}"

# 2. Create .env from template
echo ""
echo -e "${CYAN} Setting up environment file...${RESET}"
if [ ! -f .env ]; then
 cp .env.example .env
 echo -e "${GREEN} .env created — review and update passwords if needed${RESET}"
else
 echo -e "${YELLOW} .env already exists — skipping${RESET}"
fi

# 3. Create required local directories
echo ""
echo -e "${CYAN} Creating local directories...${RESET}"
mkdir -p data/raw airflow/logs spark/checkpoints
chmod +x kafka/topics/create_topics.sh
echo -e "${GREEN} Directories ready${RESET}"

# 4. Download datasets
echo ""
echo -e "${CYAN} Dataset download instructions:${RESET}"
echo -e " Download from Kaggle: https://www.kaggle.com/datasets/kartik2112/fraud-detection"
echo -e " Place files here:"
echo -e " ${GREEN}data/raw/fraudTrain.csv${RESET} (1.3M rows — historical/batch)"
echo -e " ${GREEN}data/raw/fraudTest.csv${RESET} (550K rows — streaming)"
echo ""

# 5. Git config
echo -e "${CYAN} Setting up git hooks...${RESET}"
git config core.autocrlf false # important on Linux
echo -e "${GREEN} Git configured${RESET}"

# 6. Summary
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${GREEN} Repo initialized successfully!${RESET}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""
echo -e " Next steps:"
echo -e " 1. Add your dataset CSVs to ${CYAN}data/raw/${RESET}"
echo -e " 2. Run ${CYAN}make setup${RESET} to start all services"
echo -e " 3. Open ${CYAN}http://localhost:8082${RESET} for Airflow"
echo ""
