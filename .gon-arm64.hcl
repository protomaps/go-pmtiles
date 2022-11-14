source = ["./dist/pmtiles-macos-arm64_darwin_arm64/pmtiles"]
bundle_id = "com.protomaps.pmtiles"

sign {
  application_identity = "Developer ID Application: Brandon Liu (WNSC27EEHU)"
}

zip {
  output_path = "./dist/pmtiles-darwin-arm64.zip"
}