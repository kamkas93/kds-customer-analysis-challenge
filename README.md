📊 Customer Lifecycle & Cohort Analysis – KajoDataSpace Challenge
🎯 Project Overview

Kompleksowy projekt analityczny typu End-to-End, skupiony na analizie retencji oraz wartości życiowej klienta (LTV) w modelu subskrypcyjnym. Celem było przekształcenie surowych danych transakcyjnych w strategiczny dashboard, który identyfikuje najbardziej rentowne kohorty i segmenty użytkowników.
🛠 Tech Stack & Methodology

    SQL (PostgreSQL): Zaawansowany ETL, okna czasowe (WINDOW functions), heurystyka segmentacji cenowej.

    Power BI: Modelowanie danych (Star Schema), zaawansowane miary DAX, interaktywne Bookmarki (User-Centric Design).

    Analiza Kohortowa: Monitorowanie zachowań grup użytkowników w czasie (Retention Matrix).

🧩 Phase 1: Data Engineering & Segmentation (SQL)

W tej fazie surowe dane zostały ustrukturyzowane i wzbogacone o zaawansowaną logikę biznesową:

    ETL & Cleaning: Automatyczna naprawa typów danych i walidacja kwot.

    Price Heuristics: Autorski algorytm klasyfikacji transakcji:

        Elite Annual (≥890 PLN): Segment Premium.

        Classic Monthly: Trzon przychodowy (produkty 89-249 PLN).

        Trial Starter: Identyfikacja za pomocą operacji modulo (%) i analizy wzorców (promocje/zniżki).

    Entry Segment Logic: Wykorzystanie FIRST_VALUE, aby "zamrozić" punkt wejścia klienta i poprawnie liczyć retencję po upgrade'ach.

    Customer Tiering (LTV): Nowa segmentacja oparta na sumarycznych wydatkach (Gold > 1500, Silver 500-1500, Bronze < 500).

📈 Phase 2: Strategic Insights (Power BI)

Dashboard został zaprojektowany tak, aby odpowiadać na pytania o rentowność i lojalność:
Kluczowe Funkcjonalności:

    Dynamiczna Macierz Retencji: Pozwala śledzić, jak z miesiąca na miesiąc wykruszają się poszczególne kohorty.

    Analiza LTV (Customer Tiering): Wizualizacja pokazująca, jak cena wejścia wpływa na ostateczną wartość klienta.

    Interaktywny Guide: System zakładek (Bookmarks), pełniący rolę wbudowanego słownika pojęć i metodologii.

💡 Key Business Insights

    🚀 Paradoks Trial Startera: Klienci zaczynający od najtańszych promocji wykazują zaskakująco wysoką retencję długoterminową (często wyższą niż segmenty standardowe).

    🛡️ Stabilizacja Portfela: Krytycznym momentem dla lojalności jest 3. miesiąc. Użytkownicy, którzy go przekroczą, mają 80% szans na pozostanie w ekosystemie przez kolejne pół roku.

    💎 Potencjał Smart Saver: Segment ten wykazuje najwyższy współczynnik konwersji do kategorii Gold (High Value), co czyni go kluczowym celem dla kampanii marketingowych.

📂 Repository Structure
Plaintext

├── /SQL
│   ├── 01_Data_Cleaning.sql        # Skrypty ETL i przygotowanie tabel
│   └── 02_Segmentation_Logic.sql   # Zaawansowana logika kohortowa i LTV
├── /PowerBI
│   └── KDS_Dashboard.pbix          # Plik raportu Power BI
├── /Screenshots
│   └── Dashboard_Preview.png       # Podgląd finalnego narzędzia
└── README.md
