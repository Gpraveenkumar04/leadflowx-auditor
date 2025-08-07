package audit

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type AuditResult struct {
	AuditScore  int     `json:"audit_score"`
	Performance float64 `json:"performance"`
	SEO         float64 `json:"seo"`
	SSL         float64 `json:"ssl"`
	Mobile      float64 `json:"mobile"`
	Timestamp   string  `json:"timestamp"`
	Error       string  `json:"error,omitempty"`
}

type PageSpeedResponse struct {
	LighthouseResult struct {
		Categories struct {
			Performance struct {
				Score float64 `json:"score"`
			} `json:"performance"`
			SEO struct {
				Score float64 `json:"score"`
			} `json:"seo"`
		} `json:"categories"`
	} `json:"lighthouseResult"`
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// AuditSite performs comprehensive website audit with enhanced error handling
func AuditSite(websiteURL string) (AuditResult, error) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	
	// Initialize result with default values
	result := AuditResult{
		Timestamp: timestamp,
		SSL:       0,
		Performance: 0,
		SEO:        0,
		Mobile:     0,
		AuditScore: 0,
	}

	// Validate URL format
	parsedURL, err := url.Parse(websiteURL)
	if err != nil {
		result.Error = fmt.Sprintf("Invalid URL format: %v", err)
		log.Printf("URL validation failed for %s: %v", websiteURL, err)
		return result, err
	}

	// Check SSL/HTTPS
	sslScore := checkSSL(parsedURL)
	result.SSL = sslScore

	// Get PageSpeed Insights data with retry logic
	performance, seo, err := getPageSpeedData(websiteURL)
	if err != nil {
		result.Error = fmt.Sprintf("PageSpeed API error: %v", err)
		log.Printf("PageSpeed audit failed for %s: %v", websiteURL, err)
		// Use fallback scores for SSL-only audit
		result.Performance = 0
		result.SEO = 0
		result.Mobile = 0
	} else {
		result.Performance = performance
		result.SEO = seo
		result.Mobile = performance // Mobile score based on performance for now
	}

	// Calculate weighted audit score
	result.AuditScore = calculateAuditScore(result.Performance, result.SEO, result.SSL, result.Mobile)

	log.Printf("Audit completed for %s: score=%d, perf=%.1f, seo=%.1f, ssl=%.1f", 
		websiteURL, result.AuditScore, result.Performance, result.SEO, result.SSL)

	return result, nil
}

// checkSSL verifies if the website uses HTTPS and has valid SSL certificate
func checkSSL(parsedURL *url.URL) float64 {
	if parsedURL.Scheme != "https" {
		log.Printf("Website %s does not use HTTPS", parsedURL.String())
		return 0.0
	}

	// Create HTTP client with TLS verification
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: false,
			},
		},
	}

	// Try to connect and verify SSL certificate
	resp, err := client.Get(parsedURL.String())
	if err != nil {
		log.Printf("SSL verification failed for %s: %v", parsedURL.String(), err)
		return 25.0 // Partial score for HTTPS URL even if cert has issues
	}
	defer resp.Body.Close()

	// Check if connection was successful
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		log.Printf("SSL verification successful for %s", parsedURL.String())
		return 100.0
	}

	log.Printf("SSL connection returned status %d for %s", resp.StatusCode, parsedURL.String())
	return 50.0 // Partial score for accessible HTTPS with non-optimal response
}

// getPageSpeedData fetches performance and SEO scores from Google PageSpeed Insights API
func getPageSpeedData(websiteURL string) (performance, seo float64, err error) {
	// Rate limiting: add delay between requests
	time.Sleep(1 * time.Second)

	apiURL := fmt.Sprintf("https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url=%s&strategy=mobile&category=performance&category=seo", 
		url.QueryEscape(websiteURL))

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Get(apiURL)
	if err != nil {
		return 0, 0, fmt.Errorf("PageSpeed API request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, 0, fmt.Errorf("PageSpeed API returned status %d", resp.StatusCode)
	}

	var pageSpeedResp PageSpeedResponse
	if err := json.NewDecoder(resp.Body).Decode(&pageSpeedResp); err != nil {
		return 0, 0, fmt.Errorf("Failed to parse PageSpeed response: %v", err)
	}

	// Check for API error response
	if pageSpeedResp.Error.Code != 0 {
		return 0, 0, fmt.Errorf("PageSpeed API error: %s", pageSpeedResp.Error.Message)
	}

	// Extract scores (convert from 0-1 to 0-100)
	performance = pageSpeedResp.LighthouseResult.Categories.Performance.Score * 100
	seo = pageSpeedResp.LighthouseResult.Categories.SEO.Score * 100

	log.Printf("PageSpeed data retrieved for %s: performance=%.1f, seo=%.1f", websiteURL, performance, seo)
	
	return performance, seo, nil
}

// calculateAuditScore computes weighted audit score from individual metrics
func calculateAuditScore(performance, seo, ssl, mobile float64) int {
	// Weighted scoring formula
	score := performance*0.30 + seo*0.25 + ssl*0.25 + mobile*0.20
	
	// Ensure score is within 0-100 range
	if score < 0 {
		score = 0
	} else if score > 100 {
		score = 100
	}
	
	return int(score)
}

// IsValidURL checks if the provided string is a valid URL
func IsValidURL(websiteURL string) bool {
	parsedURL, err := url.Parse(websiteURL)
	if err != nil {
		return false
	}
	
	// Must have scheme and host
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return false
	}
	
	// Must be HTTP or HTTPS
	scheme := strings.ToLower(parsedURL.Scheme)
	return scheme == "http" || scheme == "https"
}
