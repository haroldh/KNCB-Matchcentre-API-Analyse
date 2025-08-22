# 1hxws6jTapKq_iRXCRROLKr0MEQkzv2Vx4_mxjl54RtA

ACCESS_TOKEN="$(gcloud auth print-access-token \
  --impersonate-service-account=sa-matchmanagement@vra-match-management.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/spreadsheets \
  --project=vra-match-management)"

echo The token is: $ACCESS_TOKEN

echo command: curl -sfH "Authorization: Bearer ${ACCESS_TOKEN}" \
  "https://sheets.googleapis.com/v4/spreadsheets/1hxws6jTapKq_iRXCRROLKr0MEQkzv2Vx4_mxjl54RtA?fields=properties.title"


curl -sfH "Authorization: Bearer ${ACCESS_TOKEN}" \
  "https://sheets.googleapis.com/v4/spreadsheets/1hxws6jTapKq_iRXCRROLKr0MEQkzv2Vx4_mxjl54RtA?fields=properties.title"
