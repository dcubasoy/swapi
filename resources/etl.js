import swapi from 'swapi-node';
import { MongoClient } from 'mongodb';
import { newGlobalId } from 'meldio';

// Extract:

async function extract() {
  const people = await fetch('people');
  const films = await fetch('films');
  const starships = await fetch('starships');
  const vehicles = await fetch('vehicles');
  const planets = await fetch('planets');
  const species = await fetch('species');

  return { people, films, starships, vehicles, species, planets };
}

async function fetch(type) {
  const allResults = [ ];
  let response = await swapi.get(`http://swapi.co/api/${type}/`);
  const expectedCount = response.count;
  do {
    allResults.push(...response.results);
    response = await response.nextPage();
  } while (response);
  if (allResults.length !== expectedCount) {
    throw new Error(
      `Received ${allResults.length} items but ${expectedCount} was expected.`);
  }
  return allResults;
}

// Transform:

function makeIdMap(rawData, rawEntity, type) {
  return rawData[rawEntity]
    .map(person => ({[person.url]: newGlobalId(type)}))
    .reduce((acc, item) => ({...acc, ...item}), { });
}

const unknownToNull = value => value === 'unknown' ? null : value;
const noneToNull = value => value === 'none' ? null : value;
const naToNull = value => value === 'n/a' ? null : value;
const indefiniteToNull = value => value === 'indefinite' ? null : value;
const toInt = value => value ? parseInt(value, 10) : null;
const toFloat = value => value ? parseFloat(value) : null;
const toList = value => value ?
  value === 'n/a' || value === 'none' ? [ ] : value.split(', ') :
  null;

const toConsumables = value => {
  const consumablesPeriodMap = {
    hour: 'HOURS',
    hours: 'HOURS',
    day: 'DAYS',
    days: 'DAYS',
    week: 'WEEKS',
    weeks: 'WEEKS',
    month: 'MONTHS',
    months: 'MONTHS',
    year: 'YEARS',
    years: 'YEARS',
  };

  if (value === 'unknown' || value === 'none' || value === 'Live food tanks') {
    return null;
  }

  const pair = value.split(' ');
  return {
    _type: 'Duration',
    duration: parseInt(pair[0], 10),
    period: consumablesPeriodMap[pair[1]] || 'DAYS'
  };
};

const transformPerson = (idMap, person) => ({
  _id: idMap.people[person.url],
  name: person.name,
  birthYear: unknownToNull(person.birth_year),
  eyeColor: unknownToNull(person.eye_color),
  gender: noneToNull(person.gender),
  hairColor: noneToNull(person.hair_color),
  height: toInt(unknownToNull(person.height)),
  mass: toFloat(unknownToNull(person.mass)),
  skinColor: noneToNull(person.skin_color),
  created: person.created,
  edited: person.edited,
});

// preserve invariant that nodeId < relatedId
const adjustEdge = edge =>
  edge.nodeId < edge.relatedId ?
    edge :
    {
      ...edge,
      nodeId: edge.relatedId,
      nodeField: edge.relatedField,
      relatedId: edge.nodeId,
      relatedField: edge.nodeField
    };

const transformPersonConnections = (idMap, person) =>
  [ adjustEdge({
    _id: newGlobalId('_Edge'),
    nodeId: idMap.people[person.url],
    nodeField: 'homeWorlds',
    relatedId: idMap.planets[person.homeworld],
    relatedField: 'residents'
  }) ].concat(
    person.films.map(film => adjustEdge({
      _id: newGlobalId('_Edge'),
      nodeId: idMap.people[person.url],
      nodeField: 'films',
      relatedId: idMap.films[film],
      relatedField: 'characters' })))
  .concat(
    person.species.map(species => adjustEdge({
      _id: newGlobalId('_Edge'),
      nodeId: idMap.people[person.url],
      nodeField: 'species',
      relatedId: idMap.species[species],
      relatedField: 'people' })))
  .concat(
    person.starships.map(starship => adjustEdge({
      _id: newGlobalId('_Edge'),
      nodeId: idMap.people[person.url],
      nodeField: 'craft',
      relatedId: idMap.starships[starship],
      relatedField: 'pilots' })))
  .concat(
    person.vehicles.map(vehicle => adjustEdge({
      _id: newGlobalId('_Edge'),
      nodeId: idMap.people[person.url],
      nodeField: 'craft',
      relatedId: idMap.vehicles[vehicle],
      relatedField: 'pilots' })));

const transformFilm = (idMap, film) => ({
  _id: idMap.films[film.url],
  title: film.title,
  episodeId: film.episode_id,
  openingCrawl: unknownToNull(film.opening_crawl),
  director: film.director,
  producers: toList(film.producer),
  releaseDate: film.release_date,
  created: film.created,
  edited: film.edited,
});

const transformFilmConnections = (idMap, film) =>
  film.species.map(species => adjustEdge({
    _id: newGlobalId('_Edge'),
    nodeId: idMap.films[film.url],
    nodeField: 'species',
    relatedId: idMap.species[species],
    relatedField: 'films' }))
    .concat(
      film.starships.map(starship => adjustEdge({
        _id: newGlobalId('_Edge'),
        nodeId: idMap.films[film.url],
        nodeField: 'craft',
        relatedId: idMap.starships[starship],
        relatedField: 'films' })))
    .concat(
      film.vehicles.map(vehicle => adjustEdge({
        _id: newGlobalId('_Edge'),
        nodeId: idMap.films[film.url],
        nodeField: 'craft',
        relatedId: idMap.vehicles[vehicle],
        relatedField: 'films' })))
    .concat(
      film.planets.map(planet => adjustEdge({
        _id: newGlobalId('_Edge'),
        nodeId: idMap.films[film.url],
        nodeField: 'planets',
        relatedId: idMap.planets[planet],
        relatedField: 'films' })));

const transformPlanet = (idMap, planet) => ({
  _id: idMap.planets[planet.url],
  name: planet.name,
  diameter: toInt(unknownToNull(planet.diameter)),
  rotationPeriod: toInt(unknownToNull(planet.rotation_period)),
  orbitalPeriod: toInt(unknownToNull(planet.orbital_period)),
  gravity: unknownToNull(planet.gravity),
  population: toInt(unknownToNull(planet.population)),
  climates: toList(unknownToNull(planet.climate)),
  terrains: toList(unknownToNull(planet.terrain)),
  surfaceWater: toFloat(unknownToNull(planet.surface_water)),
  created: planet.created,
  edited: planet.edited
});

const transformSpecies = (idMap, species) => ({
  _id: idMap.species[species.url],
  name: species.name,
  classification: unknownToNull(species.classification),
  designation: species.designation === 'reptilian' ? 'REPTILIAN' : 'SENTIENT',
  averageHeight: toFloat(naToNull(unknownToNull(species.average_height))),
  averageLifespan:
    toInt(indefiniteToNull(unknownToNull(species.average_lifespan))),
  eyeColors: toList(unknownToNull(species.eye_colors)),
  hairColors: toList(unknownToNull(species.hair_colors)),
  skinColors: toList(unknownToNull(species.skin_colors)),
  language: unknownToNull(species.language),
  homeWorld: species.homeworld ? idMap.planets[species.homeworld] : null,
  created: species.created,
  edited: species.edited,
});

const transformStarship = (idMap, starship) => ({
  _id: idMap.starships[starship.url],
  name: starship.name,
  model: starship.model,
  class: starship.starship_class,
  manufacturers: toList(starship.manufacturer),
  costInCredits: toInt(unknownToNull(starship.cost_in_credits)),
  length: toFloat(unknownToNull(starship.length)),
  crew: unknownToNull(starship.crew),
  passengers: unknownToNull(starship.passengers),
  maxAtmosphericSpeed:
    toInt(naToNull(unknownToNull(starship.max_atmosphering_speed))),
  cargoCapacity:
    toInt(naToNull(unknownToNull(starship.cargo_capacity))),
  consumables: toConsumables(starship.consumables),
  mglt: toInt(unknownToNull(starship.MGLT)),
  hyperdriveRating: toFloat(unknownToNull(starship.hyperdrive_rating)),
  created: starship.created,
  edited: starship.edited,
});

const transformVehicle = (idMap, vehicle) => ({
  _id: idMap.vehicles[vehicle.url],
  name: vehicle.name,
  model: vehicle.model,
  class: vehicle.starship_class,
  manufacturers: toList(vehicle.manufacturer),
  costInCredits: toInt(unknownToNull(vehicle.cost_in_credits)),
  length: toFloat(unknownToNull(vehicle.length)),
  crew: unknownToNull(vehicle.crew),
  passengers: unknownToNull(vehicle.passengers),
  maxAtmosphericSpeed:
    toInt(naToNull(unknownToNull(vehicle.max_atmosphering_speed))),
  cargoCapacity:
    toInt(naToNull(unknownToNull(vehicle.cargo_capacity))),
  consumables: toConsumables(vehicle.consumables),
  created: vehicle.created,
  edited: vehicle.edited,
});

async function transform(rawData) {

  const idMap = {
    people: makeIdMap(rawData, 'people', 'Person'),
    species: makeIdMap(rawData, 'species', 'Species'),
    films: makeIdMap(rawData, 'films', 'Film'),
    planets: makeIdMap(rawData, 'planets', 'Planet'),
    starships: makeIdMap(rawData, 'starships', 'Starship'),
    vehicles: makeIdMap(rawData, 'vehicles', 'Vehicle'),
  };

  const person = rawData.people.map(transformPerson.bind(null, idMap));
  const species = rawData.species.map(transformSpecies.bind(null, idMap));
  const film = rawData.films.map(transformFilm.bind(null, idMap));
  const planet = rawData.planets.map(transformPlanet.bind(null, idMap));
  const starship = rawData.starships.map(transformStarship.bind(null, idMap));
  const vehicle = rawData.vehicles.map(transformVehicle.bind(null, idMap));
  const edges =
    rawData.people
      .map(transformPersonConnections.bind(null, idMap))
      .reduce( (acc, conns) => [ ...acc, ...conns ], [ ])
      .concat(
        rawData.films
          .map(transformFilmConnections.bind(null, idMap))
          .reduce( (acc, conns) => [ ...acc, ...conns ], [ ])
      );

  return { edges, person, species, film, planet, starship, vehicle };
}

// load:
async function loadCollection(db, collName, data) {
  const { insertedCount } = await db.collection(collName).insertMany(data);
  if (data.length !== insertedCount) {
    throw new Error(`Inserted only ${insertedCount} elements into ${collName}` +
                    ` out of ${data.length} expected.`);
  }
}

async function load(data) {
  const { edges, person, species, film, planet, starship, vehicle } = data;
  const { config: { meldio: { dbConnectionUri }}} = require('../package.json');

  const db = await MongoClient.connect(dbConnectionUri);
  await db.dropDatabase();
  await loadCollection(db, '_Edge', edges);
  await loadCollection(db, 'Film', film);
  await loadCollection(db, 'Person', person);
  await loadCollection(db, 'Planet', planet);
  await loadCollection(db, 'Species', species);
  await loadCollection(db, 'Starship', starship);
  await loadCollection(db, 'Vehicle', vehicle);
}

async function etl() {
  console.log('Extracting SWAPI Data from http://swapi.co/api...');
  const rawData = await extract();
  console.log('Transforming SWAPI Data...');
  const data = await transform(rawData);
  console.log('Loading SWAPI Data...');
  await load(data);
}

etl()
  .then( () => {
    console.log('Successfuly loaded SWAPI data.');
    process.exit(0);
  })
  .catch( e => {
    console.error('SWAPI data load failed with: ' + e.message);
    process.exit(1);
  });
