package utils

import (
	"content-management-system/src/models"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
)

// SeedWahbData seeds demo content for the Wahb Platform
func SeedWahbData(db *gorm.DB) error {
	// Check if already seeded
	var count int64
	db.Model(&models.ContentItem{}).Count(&count)
	if count > 0 {
		log.Println("Wahb data already seeded, skipping...")
		return nil
	}

	log.Println("Seeding Wahb Platform data...")

	// Seed VIDEO/PODCAST content for "For You" feed
	forYouItems := []models.ContentItem{
		createVideoContent("The Future of AI in 2026", "Thamanyah Podcast", "Radio Thamanyah", 185),
		createVideoContent("Building Scalable Systems", "Tech Talk", "Ahmed Hassan", 240),
		createVideoContent("Startup Stories: From Zero to IPO", "Entrepreneurship Weekly", "Sarah Al-Rashid", 320),
		createVideoContent("The Art of Product Design", "Design Matters", "Maya Thompson", 195),
		createVideoContent("Understanding Blockchain Technology", "Crypto Explained", "Omar Farouk", 280),
		createVideoContent("Mastering Remote Work", "Future of Work", "Lina Khoury", 165),
		createVideoContent("Mental Health in the Digital Age", "Wellness Today", "Dr. Fatima Noor", 210),
		createVideoContent("Climate Tech Innovations", "Green Future", "Yusuf Malik", 255),
		createVideoContent("The Science of Creativity", "Mind Lab", "Dr. Ali Hakim", 175),
		createVideoContent("Investing for Beginners", "Money Moves", "Hana Ibrahim", 290),
		createVideoContent("Arabic Literature Renaissance", "Cultural Corner", "Nadia Saleh", 225),
		createVideoContent("Space Exploration Updates", "Cosmic News", "Karim Mansour", 200),
		createVideoContent("Cooking Authentic Arab Cuisine", "Kitchen Stories", "Chef Layla Abbas", 340),
		createVideoContent("Learning Languages Fast", "Polyglot Tips", "Rami Khalil", 155),
		createVideoContent("Photography Masterclass", "Visual Arts", "Dina Osman", 310),
	}

	// Seed ARTICLE content for News feed (featured)
	articleItems := []models.ContentItem{
		createArticleContent("Tech Giants Announce Major AI Partnership", "Breaking news as leading technology companies form unprecedented alliance...", "Tech Daily", "James Wilson"),
		createArticleContent("Global Climate Summit Reaches Historic Agreement", "World leaders commit to ambitious carbon reduction targets...", "World News", "Maria Santos"),
		createArticleContent("Startup Ecosystem in MENA Region Thriving", "Investment in Middle Eastern startups reached record highs...", "Business Insider", "Ahmad Zaki"),
		createArticleContent("New Educational Platform Revolutionizes Learning", "AI-powered platform personalizes education for millions...", "Education Today", "Lisa Chen"),
		createArticleContent("Healthcare Innovation: Breakthrough in Gene Therapy", "Scientists achieve major milestone in treating genetic diseases...", "Medical Journal", "Dr. Robert Kim"),
		createArticleContent("Sustainable Fashion Takes Center Stage", "Major brands commit to eco-friendly manufacturing...", "Fashion Forward", "Emma Laurent"),
		createArticleContent("Space Tourism Opens to Public", "First commercial flights to orbit announced for next year...", "Space Weekly", "Michael Brown"),
		createArticleContent("Cybersecurity Threats on the Rise", "Experts warn of sophisticated new attack vectors...", "Security Today", "David Park"),
		createArticleContent("Renewable Energy Surpasses Fossil Fuels", "Historic shift in global power generation...", "Green Energy", "Sophie Anderson"),
		createArticleContent("Art Market Embraces Digital Transformation", "NFTs and digital art reshape creative industries...", "Art Review", "Carlos Mendez"),
		createArticleContent("New Economic Policy Sparks Debate", "Government announces sweeping reforms...", "Financial Times", "Rachel Green"),
		createArticleContent("Sports Technology Enhances Athlete Performance", "Wearables and analytics transform training...", "Sports Tech", "Tom Bradley"),
		createArticleContent("Urban Planning for the 21st Century", "Cities reimagine infrastructure for sustainability...", "Urban Life", "Jennifer Wang"),
		createArticleContent("Mental Wellness Apps See Surge in Users", "Digital therapy platforms gain mainstream adoption...", "Health Weekly", "Priya Sharma"),
		createArticleContent("Music Streaming Wars Intensify", "New players enter the competitive market...", "Music Biz", "Jake Morrison"),
		createArticleContent("Advances in Autonomous Vehicle Technology", "Self-driving cars edge closer to reality...", "Auto News", "Karen Lee"),
		createArticleContent("Agriculture Tech Feeds the Future", "Smart farming solutions address food security...", "Agri Tech", "Peter Okonjo"),
		createArticleContent("E-Commerce Trends Reshape Retail", "Online shopping behavior evolves post-pandemic...", "Retail Insights", "Anna Kowalski"),
		createArticleContent("5G Networks Transform Connectivity", "Ultra-fast internet enables new possibilities...", "Tech Connect", "Chris Davis"),
		createArticleContent("Social Media Regulation Debates Continue", "Lawmakers consider new oversight measures...", "Policy Watch", "Monica Rivera"),
	}

	// Seed TWEET/COMMENT content for News feed (related)
	relatedItems := []models.ContentItem{
		createTweetContent("This AI partnership is going to change everything! 🚀", "@tech_enthusiast"),
		createCommentContent("Finally some progress on climate action. Let's hold them accountable.", "Green Activist"),
		createTweetContent("Just invested in my first MENA startup. The potential is huge!", "@investor_jane"),
		createCommentContent("As a teacher, I'm excited about AI in education but we need oversight.", "Teacher_Mark"),
		createTweetContent("Gene therapy breakthrough gives hope to millions. Science wins! 🧬", "@med_news"),
		createCommentContent("Sustainable fashion isn't just a trend, it's the future.", "Fashion Lover"),
		createTweetContent("Booked my space flight! Can't believe this is real 🌟", "@space_dreamer"),
		createCommentContent("Everyone needs to take cybersecurity seriously. Update your passwords!", "Security Expert"),
		createTweetContent("Solar panels on every roof should be mandatory by now.", "@green_tech"),
		createCommentContent("NFTs are controversial but the technology is fascinating.", "Art Collector"),
		createTweetContent("New policies might actually help small businesses this time! 🤞", "@small_biz_owner"),
		createCommentContent("Sports tech is amazing but nothing beats raw talent and dedication.", "Coach Williams"),
		createTweetContent("Our cities need to be built for people, not just cars 🌳", "@urban_planner"),
		createCommentContent("Therapy apps helped me through tough times. Mental health matters!", "Recovery Journey"),
		createTweetContent("Too many streaming services now. We need consolidation!", "@music_fan"),
		createCommentContent("I'll believe self-driving cars when I see them handle snow.", "Skeptical Driver"),
		createTweetContent("Vertical farming is the future of food security 🌱", "@agri_innovator"),
		createCommentContent("Online shopping is convenient but I miss in-store experiences.", "Retail Nostalgia"),
		createTweetContent("5G finally rolled out in our area. The speed is incredible!", "@tech_user"),
		createCommentContent("Social media needs regulation but we must protect free speech too.", "Policy Watcher"),
		createTweetContent("Another great day for innovation! Keep pushing boundaries 💪", "@innovator_daily"),
		createCommentContent("This reminds me why I love following tech news.", "Casual Reader"),
		createTweetContent("Democracy depends on informed citizens. Stay curious! 📚", "@news_junkie"),
		createCommentContent("Great analysis as always. Looking forward to the next update.", "Loyal Subscriber"),
		createTweetContent("Technology should serve humanity, not the other way around.", "@ethical_tech"),
		createCommentContent("These trends are global but local impact matters most.", "Community Focus"),
		createTweetContent("Education is the best investment. Period. 🎓", "@lifelong_learner"),
		createCommentContent("Healthcare access should be universal. Period.", "Health Advocate"),
		createTweetContent("The future is looking brighter every day! ☀️", "@optimist_daily"),
		createCommentContent("Important discussion. Thanks for covering this topic.", "Engaged Reader"),
	}

	// Insert all items
	allItems := append(append(forYouItems, articleItems...), relatedItems...)
	for i := range allItems {
		if err := db.Create(&allItems[i]).Error; err != nil {
			log.Printf("Failed to seed content item: %v", err)
		}
	}

	log.Printf("Seeded %d content items for Wahb Platform", len(allItems))
	return nil
}

func createVideoContent(title, sourceName, author string, duration int) models.ContentItem {
	now := time.Now().Add(-time.Duration(rand.Intn(72)) * time.Hour)
	mediaURL := "https://cdn.wahb.app/videos/" + uuid.New().String() + ".mp4"
	thumbnailURL := "https://cdn.wahb.app/thumbnails/" + uuid.New().String() + ".jpg"

	return models.ContentItem{
		Type:         models.ContentTypeVideo,
		Source:       models.SourceTypePodcast,
		Status:       models.ContentStatusReady,
		Title:        &title,
		MediaURL:     &mediaURL,
		ThumbnailURL: &thumbnailURL,
		DurationSec:  &duration,
		Author:       &author,
		SourceName:   &sourceName,
		LikeCount:    rand.Intn(5000) + 100,
		CommentCount: rand.Intn(500) + 10,
		ShareCount:   rand.Intn(200) + 5,
		ViewCount:    rand.Intn(50000) + 1000,
		PublishedAt:  &now,
		Embedding:    func() *pgvector.Vector { v := generateMockEmbedding(); return &v }(),
	}
}

func createArticleContent(title, excerpt, sourceName, author string) models.ContentItem {
	now := time.Now().Add(-time.Duration(rand.Intn(48)) * time.Hour)
	thumbnailURL := "https://cdn.wahb.app/articles/" + uuid.New().String() + ".jpg"
	originalURL := "https://source.wahb.app/articles/" + uuid.New().String()

	return models.ContentItem{
		Type:         models.ContentTypeArticle,
		Source:       models.SourceTypeRSS,
		Status:       models.ContentStatusReady,
		Title:        &title,
		Excerpt:      &excerpt,
		ThumbnailURL: &thumbnailURL,
		OriginalURL:  &originalURL,
		Author:       &author,
		SourceName:   &sourceName,
		LikeCount:    rand.Intn(2000) + 50,
		CommentCount: rand.Intn(200) + 5,
		ShareCount:   rand.Intn(100) + 2,
		ViewCount:    rand.Intn(20000) + 500,
		PublishedAt:  &now,
		Embedding:    func() *pgvector.Vector { v := generateMockEmbedding(); return &v }(),
	}
}

func createTweetContent(text, author string) models.ContentItem {
	now := time.Now().Add(-time.Duration(rand.Intn(24)) * time.Hour)

	return models.ContentItem{
		Type:        models.ContentTypeTweet,
		Source:      models.SourceTypeManual,
		Status:      models.ContentStatusReady,
		BodyText:    &text,
		Author:      &author,
		LikeCount:   rand.Intn(500) + 10,
		ShareCount:  rand.Intn(50) + 1,
		PublishedAt: &now,
		Embedding:   func() *pgvector.Vector { v := generateMockEmbedding(); return &v }(),
	}
}

func createCommentContent(text, author string) models.ContentItem {
	now := time.Now().Add(-time.Duration(rand.Intn(12)) * time.Hour)

	return models.ContentItem{
		Type:        models.ContentTypeComment,
		Source:      models.SourceTypeManual,
		Status:      models.ContentStatusReady,
		BodyText:    &text,
		Author:      &author,
		LikeCount:   rand.Intn(100) + 1,
		PublishedAt: &now,
		Embedding:   func() *pgvector.Vector { v := generateMockEmbedding(); return &v }(),
	}
}

// generateMockEmbedding creates a random 384-dimension vector for testing
func generateMockEmbedding() pgvector.Vector {
	dims := 384
	vec := make([]float32, dims)
	for i := 0; i < dims; i++ {
		vec[i] = rand.Float32()*2 - 1 // Random values between -1 and 1
	}
	return pgvector.NewVector(vec)
}
