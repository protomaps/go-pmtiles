source = ["./dist/pmtiles-macos-amd64_darwin_amd64_v1/pmtiles"]
bundle_id = "com.protomaps.pmtiles"

apple_id {
  username = "@env:AC_USERNAME"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: Brandon Liu (WNSC27EEHU)"
}

zip {
  output_path = "./dist/pmtiles-darwin-amd64.zip"
}

dmg {
  output_path = "./dist/pmtiles-darwin-amd64.dmg"
  volume_name = "pmtiles"
}