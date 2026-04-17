Customer Lifecycle & Cohort Analysis – KajoDataSpace Challenge

Project Overview

Analiza retencji i wartości życiowej klienta (LTV) oparta na danych transakcyjnych. Celem projektu było zidentyfikowanie kluczowych segmentów napędzających wzrost oraz zrozumienie, jak promocje i modele subskrypcyjne wpływają na lojalność użytkowników.

🛠 Tech Stack

    SQL (PostgreSQL): ETL, czyszczenie danych, logika kohortowa i segmentacja behawioralna.

    Power BI: Zaawansowana wizualizacja, analiza Kohort (Matrix) oraz Dashboarding strategiczny.

Phase 1: Data Engineering (SQL)

W tej fazie przekształciłem surowe, nieustrukturyzowane dane w relacyjną bazę gotową do analizy BI.
Key SQL Achievements:

    ETL & Data Cleaning: Naprawa formatów dat oraz konwersja separatorów dziesiętnych (z 19,99 na 19.99 numeric).

    Cohort Logic: Obliczenie month_number dla każdej transakcji względem daty pozyskania klienta.

    Behavioral Segmentation: Automatyczne przypisanie statusu New vs Returning na poziomie rekordu.

    Price Clustering: Autorska segmentacja cenowa pozwalająca odróżnić "Łowców promocji" od klientów "Elite Annual".

📊 Phase 2: Zaawansowana Analityka Kohortowa i Segmentacja (SQL & Power BI)

Głównym celem projektu było zidentyfikowanie najbardziej rentownych ścieżek klienta (Customer Journey) w ekosystemie KajoDataSpace. W tym celu wdrożono autorską logikę segmentacji oraz model retencji oparty na kohortach.
🧩 Metodologia Segmentacji (Pattern Recognition)

Zamiast prostego przypisywania kwot, zastosowałem logikę Heurystyki Cenowej w SQL, która automatycznie klasyfikuje transakcje na podstawie ich struktury i wzorców:

Elite Annual (≥890 PLN): Segment High-Ticket. Klienci o najwyższym LTV, płacący z góry za dostęp roczny.

Classic Monthly: Główne produkty subskrypcyjne (89, 99, 169, 199, 249 PLN). Trzon przychodowy biznesu.

Trial Starter: Kluczowy lejek akwizycyjny. Wyodrębniony za pomocą logiki:

Kwoty wejściowe (<89 PLN)

Niestandardowe końcówki cenowe (np. grosze), identyfikowane przez operator modulo (%) i analizę tekstu, wskazujące na dynamiczne zniżki procentowe.

Smart Saver & Basic Access: Pakiety wielomiesięczne oraz transakcje uzupełniające.

📈 Kluczowe Funkcje Modelu Danych

W celu uzyskania prawdziwego obrazu lojalności, model SQL został wzbogacony o:

Logikę Entry Segment: Przy wykorzystaniu funkcji okna FIRST_VALUE, każdy klient jest analizowany przez pryzmat swojej "ceny wejścia". Zapobiega to błędom w wykresach retencji, gdzie migracja klienta do droższego planu byłaby błędnie interpretowana jako odejście (churn).

Analizę Kohortową: Dynamiczne obliczanie month_number dla każdej transakcji, co pozwoliło na stworzenie macierzy retencji monitorującej stabilność bazy użytkowników w czasie.

💡 Główne Wnioski Biznesowe

Paradoks Trial Startera: Analiza wykazała, że segment promocyjny, mimo najniższej bazy wejściowej, generuje najwyższą retencję długoterminową. Strategia "taniego wejścia" skutecznie buduje lojalnych subskrybentów.

Stabilizacja Retencji: Punkt krytyczny przypada na 3-4 miesiąc. Użytkownicy, którzy przejdą ten próg, wykazują wysoką stabilność płatniczą (retencja na poziomie ~30%).

Optymalizacja Przychodów: Największy potencjał do optymalizacji cenowej leży w segmencie Classic Monthly ze względu na jego skalę i odporność na jednostkowe odejścia.

📂 Repository Structure

    /SQL: Skrypty czyszczące i tworzące widoki analityczne.

    /PowerBI: Plik .pbix z interaktywnym raportem.

    /Screenshots: Wizualizacje i PDF z dashboardem.
