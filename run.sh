gcloud auth login
gcloud auth application-default login
gcloud config set project vra-match-management

# eenmalig (als nog niet gedaan):
gcloud iam service-accounts add-iam-policy-binding \
  sa-matchmanagement@vra-match-management.iam.gserviceaccount.com \
  --role roles/iam.serviceAccountTokenCreator \
  --member user:haroldhorsman@vra.nl

# runnen
#export GOOGLE_IMPERSONATE_SERVICE_ACCOUNT=sa-matchmanagement@vra-match-management.iam.gserviceaccount.com
#export SEASON_ID=19
#export RV_ID=134453
# ... (je overige niet-gevoelige env’s)
npm ci
node fetchmatches-master.mjs
