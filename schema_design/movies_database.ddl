CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT not null,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

 CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

 CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id UUID PRIMARY KEY,
    genre_id UUID NOT NULL,
    film_work_id UUID NOT NULL,
    created TIMESTAMP WITH TIME ZONE,

    CONSTRAINT fk_genre FOREIGN KEY (genre_id) REFERENCES genre(id) ON DELETE CASCADE,
    CONSTRAINT fk_film_work FOREIGN KEY (film_work_id) REFERENCES film_work(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id UUID PRIMARY KEY,
    person_id UUID NOT NULL,
    film_work_id UUID NOT NULL,
    role TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,

    CONSTRAINT fk_person FOREIGN KEY (person_id) REFERENCES person(id) ON DELETE CASCADE,
    CONSTRAINT fk_film_work FOREIGN KEY (film_work_id) REFERENCES film_work(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_genre_film_work_genre_id ON content.genre_film_work(genre_id);
CREATE INDEX IF NOT EXISTS idx_genre_film_work_film_work_id ON content.genre_film_work(film_work_id);
CREATE INDEX IF NOT EXISTS idx_person_film_work_person_id ON content.person_film_work(person_id);
CREATE INDEX IF NOT EXISTS idx_person_film_work_film_work_id ON content.person_film_work(film_work_id);
CREATE INDEX IF NOT EXISTS idx_film_work_title ON content.film_work(title);
CREATE INDEX IF NOT EXISTS idx_film_work_creation_date ON content.film_work(creation_date);
CREATE INDEX IF NOT EXISTS idx_film_work_rating ON content.film_work(rating);