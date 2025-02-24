#!/bin/bash
#
# This script will:
#   1) Install mailutils + sSMTP
#   2) Configure sSMTP for Gmail
#   3) Send a test email
#   4) Prompt for and run a command
#   5) Send an email notification after that command completes
#
# Usage:
#   sudo ./setup_notify_run.sh
#

###############################################################################
# 1) Ensure script is run as root
###############################################################################
if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: This script must be run as root (try: sudo $0)"
  exit 1
fi

###############################################################################
# 2) Install required packages
###############################################################################
echo "[INFO] Updating apt-get and installing mailutils & sSMTP..."
apt-get update -y && apt-get install -y mailutils ssmtp

###############################################################################
# 3) Collect Gmail credentials and configure sSMTP
###############################################################################
echo ""
echo "[INFO] Configuring sSMTP for Gmail."
echo "       You need a Gmail account and, if you use 2FA, a Gmail App Password."
echo ""

read -p "Enter your Gmail address (e.g. user@gmail.com): " GMAIL_ADDRESS
read -s -p "Enter your Gmail App Password: " GMAIL_APP_PASS
echo ""

# Hostname for email headers
HOSTNAME=$(hostname)

cat <<EOF > /etc/ssmtp/ssmtp.conf
# sSMTP configuration for sending mail via Gmail
root=$GMAIL_ADDRESS
mailhub=smtp.gmail.com:587
AuthUser=$GMAIL_ADDRESS
AuthPass=$GMAIL_APP_PASS
UseSTARTTLS=YES
FromLineOverride=YES
rewriteDomain=gmail.com
hostname=$HOSTNAME
EOF

# Restrict permissions to protect your credentials
chmod 600 /etc/ssmtp/ssmtp.conf

###############################################################################
# 4) Send a test email to confirm configuration
###############################################################################
echo ""
echo "[INFO] Sending a test email to $GMAIL_ADDRESS..."
TEST_SUBJECT="sSMTP Test from $HOSTNAME"
TEST_BODY="This is a test email from $HOSTNAME using sSMTP."
echo "$TEST_BODY" | mail -s "$TEST_SUBJECT" "$GMAIL_ADDRESS"

echo "[INFO] A test email has been sent. Please check your inbox."
read -p "Press Enter to continue..."

###############################################################################
# 5) Prompt user for the command to run
###############################################################################
echo ""
echo "Which command do you want to run? (e.g. 'sleep 10' or 'apt-get upgrade')"
read -p "Command: " USER_COMMAND

if [ -z "$USER_COMMAND" ]; then
  echo "No command entered. Exiting."
  exit 1
fi

###############################################################################
# 6) Run the command and capture exit code
###############################################################################
echo ""
echo "[INFO] Running: $USER_COMMAND"
# Using 'bash -c' so the user can type a multi-word command with flags/spaces
bash -c "$USER_COMMAND"
STATUS=$?

###############################################################################
# 7) Send email notification about completion
###############################################################################
FINISH_SUBJECT="[Notification] Command finished on $HOSTNAME"
FINISH_BODY="The command \"$USER_COMMAND\" finished with exit code $STATUS on $HOSTNAME."
echo "$FINISH_BODY" | mail -s "$FINISH_SUBJECT" "$GMAIL_ADDRESS"

echo ""
echo "[INFO] Done! A completion email has been sent to $GMAIL_ADDRESS."
echo "[INFO] Exit code of the command was: $STATUS"
exit $STATUS
