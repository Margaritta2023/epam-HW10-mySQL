const { Client } = require('pg');

// Define the PostgreSQL connection parameters
const defaultClientConfig = {
  user: 'postgres',  
  host: 'localhost',
  password: '123456',  
  port: 5432,
};

const dbName = 'movies_db';

const createDatabaseAndTables = async () => {
  const defaultClient = new Client(defaultClientConfig);
  
  try {
    // Connect to PostgreSQL
    await defaultClient.connect();
    console.log('Connected to PostgreSQL');

    // Create the database if it does not exist
    await defaultClient.query(`CREATE DATABASE ${dbName}`);
    console.log(`Database ${dbName} created`);

    // Disconnect from the default database
    await defaultClient.end();

    // Connect to the new database
    const newClientConfig = {
      ...defaultClientConfig,
      database: dbName,
    };

    const newClient = new Client(newClientConfig);
    await newClient.connect();
    console.log('Connected to the new database');

    // Define tables to be created
    const createTablesQuery = `
      CREATE TABLE IF NOT EXISTS Directors (
        DirectorID SERIAL PRIMARY KEY,
        Name VARCHAR(100) NOT NULL,
        Nationality VARCHAR(50),
        DOB DATE
      );

      CREATE TABLE IF NOT EXISTS Actors (
        ActorID SERIAL PRIMARY KEY,
        Name VARCHAR(100) NOT NULL,
        Nationality VARCHAR(50),
        DOB DATE
      );

      CREATE TABLE IF NOT EXISTS Genres (
        GenreID SERIAL PRIMARY KEY,
        GenreName VARCHAR(50) NOT NULL
      );

      CREATE TABLE IF NOT EXISTS Movies (
        MovieID SERIAL PRIMARY KEY,
        Title VARCHAR(100) NOT NULL,
        ReleaseYear INT,
        DirectorID INT REFERENCES Directors(DirectorID)
      );

      CREATE TABLE IF NOT EXISTS Ratings (
        MovieID INT REFERENCES Movies(MovieID),
        Rating DECIMAL(3, 1),
        PRIMARY KEY (MovieID)
      );

      CREATE TABLE IF NOT EXISTS MovieGenres (
        MovieID INT REFERENCES Movies(MovieID),
        GenreID INT REFERENCES Genres(GenreID),
        PRIMARY KEY (MovieID, GenreID)
      );
    `;

    // Create tables
    await newClient.query(createTablesQuery);
    console.log('Tables created');

    // Define data insertion queries with new sample data
    const insertDataQuery = `
      -- Insert sample Directors
      INSERT INTO Directors (Name, Nationality, DOB) VALUES
        ('Alfred Hitchcock', 'British', '1899-08-13'),
        ('Akira Kurosawa', 'Japanese', '1910-03-23'),
        ('Hayao Miyazaki', 'Japanese', '1941-01-05'),
        ('Ridley Scott', 'British', '1937-11-30'),
        ('David Fincher', 'American', '1962-08-28')
      ON CONFLICT (DirectorID) DO NOTHING;

      -- Insert sample Actors
      INSERT INTO Actors (Name, Nationality, DOB) VALUES
        ('Harrison Ford', 'American', '1942-07-13'),
        ('Meryl Streep', 'American', '1949-06-22'),
        ('Jean Reno', 'French', '1948-07-30'),
        ('Cate Blanchett', 'Australian', '1969-05-14'),
        ('Javier Bardem', 'Spanish', '1969-03-01')
      ON CONFLICT (ActorID) DO NOTHING;

      -- Insert sample Genres
      INSERT INTO Genres (GenreName) VALUES
        ('Horror'),
        ('Action'),
        ('Animation'),
        ('Sci-Fi'),
        ('Documentary')
      ON CONFLICT (GenreID) DO NOTHING;

      -- Insert sample Movies
      INSERT INTO Movies (Title, ReleaseYear, DirectorID) VALUES
        ('Psycho', 1960, 1),
        ('Seven Samurai', 1954, 2),
        ('Spirited Away', 2001, 3),
        ('Blade Runner', 1982, 4),
        ('Fight Club', 1999, 5)
      ON CONFLICT (MovieID) DO NOTHING;

      -- Insert sample Ratings
      INSERT INTO Ratings (MovieID, Rating) VALUES
        (1, 8.5),
        (2, 8.6),
        (3, 8.8),
        (4, 8.1),
        (5, 8.8)
      ON CONFLICT (MovieID) DO NOTHING;

      -- Insert sample MovieGenres
      INSERT INTO MovieGenres (MovieID, GenreID) VALUES
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 2),
        (5, 5)
      ON CONFLICT (MovieID, GenreID) DO NOTHING;
    `;

    // Insert data into tables
    await newClient.query(insertDataQuery);
    console.log('Data inserted');

    // Disconnect from the new database
    await newClient.end();
  } catch (error) {
    console.error('Error:', error);
  }
};

createDatabaseAndTables();
