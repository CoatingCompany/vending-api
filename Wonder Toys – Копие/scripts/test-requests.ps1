\
    # Example test calls (run after API is up)
    $Headers = @{ "x-api-key" = $env:API_KEY }
    Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8000/health"
    Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/append" `
        -Headers $Headers -ContentType "application/json" `
        -Body '{"location":"Kaufland Plovdiv","product":"Squishies","quantity":40,"note":"refilled","user":"MK"}'
    Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8000/last-product?location=Kaufland%20Plovdiv" -Headers $Headers
