stage('Download & Unzip Latest Per Prefix, Then Sort Ascending') {
    steps {
        sh '''
            set -ex
            mkdir -p /tmp/data /tmp/extracted

            # 1) List all .zip files in the bucket, sorted by time descending
            aws s3 ls s3://${BUCKET_NAME}/ --recursive | grep '\\.zip$' | sort -k1,2r | awk '{print $4}' > all_zips.txt

            # 2) Keep only the first occurrence of each prefix => the "latest" zip for that prefix
            awk -F'_' '!seen[$1]++' all_zips.txt > latest_per_prefix.txt

            # 3) Parse prefix, strip leading zeros, sort ascending by prefix
            awk -F'_' '{
                original=$0
                prefix=$1
                sub(/^0+/, "", prefix)    # remove leading zeros
                if(prefix=="") prefix=0   # handle "0000" => 0
                print prefix, original
            }' latest_per_prefix.txt | sort -k1,1n | awk '{print $2}' > sorted_latest_per_prefix.txt

            # 4) Download & extract each ZIP into /tmp/extracted/<prefix>/
            while read -r zipFile; do
                echo "Processing $zipFile"

                # Derive prefix from the file name (the part before '_')
                rawPrefix=$(echo "$zipFile" | awk -F'_' '{print $1}')
                # Strip leading zeros
                prefix=$(echo "$rawPrefix" | sed 's/^0*//')
                if [ -z "$prefix" ]; then
                    prefix=0
                fi

                # Create subfolder /tmp/extracted/<prefix>
                storeFolder="/tmp/extracted/$prefix"
                mkdir -p "$storeFolder"

                # Download the zip
                aws s3 cp "s3://${BUCKET_NAME}/$zipFile" /tmp/data/temp.zip

                # Unzip into /tmp/extracted/<prefix>, preserving folder structure
                # (no -j, so if there's a 'data/' folder inside, it remains)
                unzip -o /tmp/data/temp.zip -d "$storeFolder" --quiet || echo "No .dbf found in $zipFile, skipping"

                rm -f /tmp/data/temp.zip
            done < sorted_latest_per_prefix.txt
        '''
    }
}
