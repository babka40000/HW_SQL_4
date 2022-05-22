import sqlalchemy
from pprint import pprint
import psycopg2

if __name__ == '__main__':
    db = 'postgresql://maks:ghbdtnbr@localhost:5432/test'
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect()

    request_delete_all_data = '''
        DELETE FROM artistalbum;
        DELETE FROM artistgenre;
        DELETE FROM collectiontrek;
        DELETE FROM trek;
        DELETE FROM genre;
        DELETE FROM album;
        DELETE FROM artist; 
        DELETE FROM collection;
    '''

    request_add_genre = '''
            INSERT INTO genre(id, name) VALUES(1, 'русский рок');
            INSERT INTO genre(id, name) VALUES(2, 'поп');
            INSERT INTO genre(id, name) VALUES(3, 'панк рок');
            INSERT INTO genre(id, name) VALUES(4, 'индастриал');
            INSERT INTO genre(id, name) VALUES(5, 'рок');
            INSERT INTO genre(id, name) VALUES(6, 'танцевальная музыка');
            INSERT INTO genre(id, name) VALUES(7, 'хеви-метал');
            INSERT INTO genre(id, name) VALUES(8, 'хард-рок');
            INSERT INTO genre(id, name) VALUES(9, 'брейк-бит');
        '''

    request_add_artist = '''
            INSERT INTO artist(id, name, alias) VALUES(1, 'Мумий Тролль', 'Лагутенко');
            INSERT INTO artist(id, name, alias) VALUES(2, 'Би-2', 'Шура и Лева');
            INSERT INTO artist(id, name, alias) VALUES(3, 'Natalie Imbruglia', 'Natalie Imbruglia');
            INSERT INTO artist(id, name, alias) VALUES(4, 'Король и Шут', 'Горшок и Князь');
            INSERT INTO artist(id, name, alias) VALUES(5, 'Rammstein', 'Раммы');
            INSERT INTO artist(id, name, alias) VALUES(6, 'David Guetta', 'David Guetta');
            INSERT INTO artist(id, name, alias) VALUES(7, 'Metallica', 'Metallica');
            INSERT INTO artist(id, name, alias) VALUES(8, 'AC/DC', 'AC/DC');
            INSERT INTO artist(id, name, alias) VALUES(9, 'The Prodigy', 'The Prodigy');
         '''

    request_add_artist_genre = '''
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(1, 1);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(2, 1);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(3, 2);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(4, 3);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(4, 1);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(5, 4);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(5, 5);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(6, 6);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(7, 7);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(7, 5);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(8, 5);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(8, 8);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(9, 9);
            INSERT INTO artistgenre(artist_id, genre_id) VALUES(9, 6);
        '''

    request_add_album = '''
            INSERT INTO album(id, name, release) VALUES(1, 'Best 20-20', '01-01-2020');
            INSERT INTO album(id, name, release) VALUES(2, 'Super Izlase V', '01-01-2014');
            INSERT INTO album(id, name, release) VALUES(3, 'Малыш', '01-01-1999');
            INSERT INTO album(id, name, release) VALUES(4, 'Би-2', '01-01-2000');
            INSERT INTO album(id, name, release) VALUES(5, '16плюс', '01-01-2014');
            INSERT INTO album(id, name, release) VALUES(6, 'Glorious: The Singles 97-07', '01-01-2007');
            INSERT INTO album(id, name, release) VALUES(7, 'Reload', '01-01-2003');
            INSERT INTO album(id, name, release) VALUES(8, 'Акустический альбом', '01-01-1998');
            INSERT INTO album(id, name, release) VALUES(9, 'Как в старой сказке', '01-01-2001');
            INSERT INTO album(id, name, release) VALUES(10, 'Продавец кошмаров(', '01-01-2006');
            INSERT INTO album(id, name, release) VALUES(11, 'Rammstein', '01-01-2019');
            INSERT INTO album(id, name, release) VALUES(12, 'Mutter', '01-01-2001');
            INSERT INTO album(id, name, release) VALUES(13, 'Reise, Reise', '01-01-2004');
            INSERT INTO album(id, name, release) VALUES(14, 'Listen Again', '01-01-2015');
            INSERT INTO album(id, name, release) VALUES(15, 'One More Love', '01-01-2010');
            INSERT INTO album(id, name, release) VALUES(16, 'Load', '01-01-1996');
            INSERT INTO album(id, name, release) VALUES(17, 'Death Magnetic', '01-01-2008');
            INSERT INTO album(id, name, release) VALUES(18, 'The Razors Edge', '01-01-1990');
            INSERT INTO album(id, name, release) VALUES(19, 'Black Ice', '01-01-2008');
            INSERT INTO album(id, name, release) VALUES(20, 'Invaders Must Die', '01-01-2009');
            INSERT INTO album(id, name, release) VALUES(21, 'No Tourists', '01-01-2018');
        '''

    request_add_artist_album = '''
            INSERT INTO artistalbum(artist_id, album_id) VALUES(1, 1);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(1, 2);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(1, 3);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(2, 4);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(2, 5);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(3, 6);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(3, 7);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(4, 8);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(4, 9);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(4, 10);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(5, 11);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(5, 12);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(5, 13);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(6, 14);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(6, 15);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(7, 16);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(7, 17);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(8, 18);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(8, 19);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(9, 20);
            INSERT INTO artistalbum(artist_id, album_id) VALUES(9, 21);
        '''

    request_add_trek = '''
            INSERT INTO trek(id, album_id, name, duration) VALUES(1, 1, 'Владивосток 2000', 158);
            INSERT INTO trek(id, album_id, name, duration) VALUES(2, 2, 'Медведица', 231);
            INSERT INTO trek(id, album_id, name, duration) VALUES(3, 3, 'Малыш', 121);
            INSERT INTO trek(id, album_id, name, duration) VALUES(4, 4, 'Полковнику никто не пишет', 292);
            INSERT INTO trek(id, album_id, name, duration) VALUES(5, 4, 'Варвара', 301);
            INSERT INTO trek(id, album_id, name, duration) VALUES(6, 5, 'Компромисс', 267);
            INSERT INTO trek(id, album_id, name, duration) VALUES(7, 6, 'Torn', 244);
            INSERT INTO trek(id, album_id, name, duration) VALUES(8, 7, 'Never Tear Us Apart', 188);
            INSERT INTO trek(id, album_id, name, duration) VALUES(9, 6, 'Big Mistake', 273);
            INSERT INTO trek(id, album_id, name, duration) VALUES(10, 8, 'Кукла колдуна', 203);
            INSERT INTO trek(id, album_id, name, duration) VALUES(11, 9, 'Проклятый старый дом', 257);
            INSERT INTO trek(id, album_id, name, duration) VALUES(12, 10, 'Ром', 161);
            INSERT INTO trek(id, album_id, name, duration) VALUES(13, 11, 'DEUTSCHLAND', 322);
            INSERT INTO trek(id, album_id, name, duration) VALUES(14, 12, 'Feuer Frei!', 189);
            INSERT INTO trek(id, album_id, name, duration) VALUES(15, 13, 'Ohne Dich', 271);
            INSERT INTO trek(id, album_id, name, duration) VALUES(16, 14, 'Dangerous', 203);
            INSERT INTO trek(id, album_id, name, duration) VALUES(17, 14, 'Lovers on the Sun', 203);
            INSERT INTO trek(id, album_id, name, duration) VALUES(18, 15, 'Sexy Bitch', 195);
            INSERT INTO trek(id, album_id, name, duration) VALUES(19, 16, 'Mama Said', 321);
            INSERT INTO trek(id, album_id, name, duration) VALUES(20, 16, 'Bleeding my', 498);
            INSERT INTO trek(id, album_id, name, duration) VALUES(21, 17, 'The Day That Never Comes', 476);
            INSERT INTO trek(id, album_id, name, duration) VALUES(22, 18, 'Thunderstruck', 292);
            INSERT INTO trek(id, album_id, name, duration) VALUES(23, 19, 'Rock N Roll Train', 271);
            INSERT INTO trek(id, album_id, name, duration) VALUES(24, 18, 'Are You Ready', 250);
            INSERT INTO trek(id, album_id, name, duration) VALUES(25, 20, 'Omen', 216);
            INSERT INTO trek(id, album_id, name, duration) VALUES(26, 20, 'Invaders Must Die', 295);
            INSERT INTO trek(id, album_id, name, duration) VALUES(27, 21, 'Timebomb Zone', 204);
        '''

    request_add_collection = '''
            INSERT INTO collection(id, name, release) VALUES(1, 'Рок хиты 1', '01-01-2018');
            INSERT INTO collection(id, name, release) VALUES(2, 'Рок хиты 2', '01-01-2004');
            INSERT INTO collection(id, name, release) VALUES(3, 'Рок хиты 3', '01-01-2014');
            INSERT INTO collection(id, name, release) VALUES(4, 'Танцевальные хиты 1', '01-01-2020');
            INSERT INTO collection(id, name, release) VALUES(5, 'Танцевальные хиты 2', '01-01-2015');
            INSERT INTO collection(id, name, release) VALUES(6, 'Попса', '01-01-2000');
            INSERT INTO collection(id, name, release) VALUES(7, 'Русский рок 1', '01-01-2015');
            INSERT INTO collection(id, name, release) VALUES(8, 'Русский рок 2', '01-01-2020');
        '''

    request_add_collection_trek = '''
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(2, 1);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(12, 1);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(20, 1);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(22, 2);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(20, 2);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(5, 2);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(10, 3);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(20, 3);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(12, 3);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(17, 4);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(18, 4);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(27, 4);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(26, 5);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(25, 5);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(18, 5);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(7, 6);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(9, 6);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(18, 6);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(2, 7);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(5, 7);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(10,7);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(12, 8);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(6, 8);
                INSERT INTO collectiontrek(trek_id, collection_id) VALUES(1, 8);
            '''

    fill_table = connection.execute(request_delete_all_data +
                                    request_add_genre +
                                    request_add_artist +
                                    request_add_artist_genre +
                                    request_add_album +
                                    request_add_artist_album +
                                    request_add_trek +
                                    request_add_collection +
                                    request_add_collection_trek)

    request_album_2018_year = '''
            SELECT name FROM album WHERE release='01-01-2018';
    '''
    print('Альбомы, выпущенные в 2018 году:')
    sel = connection.execute(request_album_2018_year)
    pprint(sel.fetchall())
    print()

    request_the_longest_trek = '''
                SELECT name,duration FROM trek ORDER BY duration DESC LIMIT 1;
        '''
    print('Самый длинный трек:')
    sel = connection.execute(request_the_longest_trek).fetchall()
    print(f'Трек - {sel[0][0]}, длина - {sel[0][1]} секунд')
    print()

    request_treks_more_3_5 = '''
                    SELECT name,duration FROM trek WHERE duration >= 210;
            '''
    print('Треки, больше 3,5 минут:')
    sel = connection.execute(request_treks_more_3_5).fetchall()
    for trek in sel:
        print(f'{trek[0]}, длина - {trek[1]} секунд')
    print()

    request_collection_2018_2020_year = '''
                        SELECT name,release FROM collection WHERE release BETWEEN '01-01-2018' AND '01-01-2020';
                '''
    print('Сборники, выпущенные между 2018 и 2020 годами:')
    sel = connection.execute(request_collection_2018_2020_year).fetchall()
    for collect in sel:
        print(f'Сборник "{collect[0]}", год выпуска - {str(collect[1])[0:4]}')
    print()

    request_artist_one_word = '''
                            SELECT name FROM artist WHERE name NOT LIKE '%% %%';
                    '''
    print('Артисты, у которых имя состоит из одного слова:')
    sel = connection.execute(request_artist_one_word).fetchall()
    for artist in sel:
        print(f'{artist[0]}')
    print()

    request_treks_contains_my = '''
                        SELECT name FROM trek WHERE name LIKE '%%my%%' OR name LIKE '%%мой%%';
                '''
    print('Треки, которые содержат "my" или "мой":')
    sel = connection.execute(request_treks_contains_my).fetchall()
    for trek in sel:
        print(f'{trek[0]}')