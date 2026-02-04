-- Enable pgcrypto for gen_random_uuid
create extension if not exists pgcrypto;

-- pages
create table if not exists pages (
    id serial primary key,
    public_id uuid default gen_random_uuid() unique,
    title varchar(255) not null,
    content text not null,
    created_at timestamptz default current_timestamp,
    updated_at timestamptz default current_timestamp
);

-- media
create table if not exists media (
    id serial primary key,
    public_id uuid default gen_random_uuid() unique,
    url varchar(255) not null,
    type varchar(50),
    created_at timestamptz default current_timestamp,
    updated_at timestamptz default current_timestamp
);

-- posts
create table if not exists posts (
    id serial primary key,
    public_id uuid default gen_random_uuid() unique,
    title varchar(255) not null,
    content text not null,
    author varchar(255) not null,
    created_at timestamptz default current_timestamp,
    updated_at timestamptz default current_timestamp
);

-- post_media (many-to-many)
create table if not exists post_media (
    post_id integer not null,
    media_id integer not null,
    primary key (post_id, media_id),
    constraint fk_post_media_post foreign key (post_id) references posts(id) on delete cascade,
    constraint fk_post_media_media foreign key (media_id) references media(id) on delete cascade
);

-- admin_users
create table if not exists admin_users (
    id serial primary key,
    public_id uuid default gen_random_uuid() unique not null,
    email varchar(255) not null unique,
    role varchar(50) not null,
    password_hash text not null,
    permissions text[],
    is_active boolean default true,
    created_at timestamptz default current_timestamp,
    updated_at timestamptz default current_timestamp
);

create index if not exists idx_admin_users_public_id on admin_users(public_id);
